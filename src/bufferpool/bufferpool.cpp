// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/14/22.
//

#include <cstring>
#include <chrono>
#include "bufferpool.h"

bufferPoolManager::bufferPoolManager(storageManager *stmgr) {
  this->stmgr_ = stmgr;
  this->pages_ = std::vector<heapPage>(this->no_pages_);
  for (unsigned int cnt = 0; cnt < this->no_pages_; cnt++) {
	this->page_table_.insert(std::pair(&this->pages_[cnt], INVALID_PHYSICAL_PAGE_ID));
  }
  this->is_dirty_ = std::vector<bool>(this->no_pages_);
  this->pin_cnt_ = std::vector<size_t>(this->no_pages_);
}

heapPage *bufferPoolManager::evict() {
  /*
   * evict the oldest page, return the pointer to the evicted page
   */
  std::time_t oldest = std::time(nullptr);
  unsigned int cnt, idx = 0;
  bool found = false;
  for (cnt = 0; cnt < this->no_pages_; ++cnt) {
	if (this->pin_cnt_[cnt] == 0 and this->pages_[cnt].timestamp < oldest) {
	  idx = cnt;
	  found = true;
	}
  }
  if (not found) {
	return nullptr;
  }
  if (this->is_dirty_[idx]) {
	// write dirty page to disk before evict it;
	if (this->page_table_[&this->pages_[idx]] == INMEMORY_PAGE_ID) {
	  // need to assign a page id for this view
	  this->page_table_[&this->pages_[idx]] = this->stmgr_->addPage();
	}
	this->writeToDisk(this->page_table_[&this->pages_[idx]], idx);
  }
  heapPage *cleanPage = &this->pages_[idx];
  std::memset(cleanPage->content, 0, HEAP_SIZE);
  cleanPage->timestamp = std::time(nullptr);
  cleanPage->data_ptr = cleanPage->content + HEAP_SIZE;
  cleanPage->data_ptr = cleanPage->content;
  cleanPage->data_cnt = 0;
  this->pin_cnt_[idx] = 0;
  this->page_table_[cleanPage] = INVALID_PHYSICAL_PAGE_ID;
  return &this->pages_[idx];
}

void bufferPoolManager::readFromDisk(PhysicalPageID psy_id) {
  // This method should only be called after checking whether the page is already in buffer by calling findPage
  heapPage *curPage;
  size_t cnt;
  std::tie(curPage, cnt) = this->nextFreePage();
  if (curPage != nullptr) {
	this->page_table_[curPage] = psy_id;
	this->stmgr_->readPage(psy_id, curPage->content);
	curPage->timestamp = std::time(nullptr);
	curPage->data_cnt = this->stmgr_->getDataCnt(psy_id);
	curPage->data_ptr = curPage->content + HEAP_SIZE;
	curPage->idx_ptr = curPage->content;
	this->pin_cnt_[cnt] += 1;
	this->is_dirty_[cnt] = false;
	return;
  }
  heapPage *cleanedPage = evict();
  this->stmgr_->readPage(psy_id, cleanedPage->content);
  cleanedPage->data_cnt = this->stmgr_->getDataCnt(psy_id);
  this->page_table_[cleanedPage] = psy_id;
}

void bufferPoolManager::writeToDisk(PhysicalPageID psy_id, size_t idx) {
  this->stmgr_->writePage(psy_id, this->pages_[idx].content);
  this->is_dirty_[idx] = false; // reset is_dirty_ flag
  if (this->pin_cnt_[idx] > 0) {
	--this->pin_cnt_[idx];
  } else {
	this->pin_cnt_[idx] = 0;
  } // by default, decrease pin_cnt_ by 1 CAUTION
}

void bufferPoolManager::insertToFrame(int idx, const char *buf, size_t len) {
  if (not this->pages_[idx].insert(buf, len)) {
	if (idx != BUFFER_POOL_SIZE) {
	  this->insertToFrame(++idx, buf, len);
	} else {
	  heapPage *free_page = evict();
	  this->insertToFrame(free_page, buf, len);
	}
  }
  this->is_dirty_[idx] = true;
}

void bufferPoolManager::insertToFrame(heapPage *cur_page, const char *buf, size_t len) {
  if (not cur_page->insert(buf, len)) {
	if (cur_page->id != this->pages_.cend()->id) {
	  this->insertToFrame(cur_page + sizeof(heapPage), buf, len);
	} else {
	  heapPage *free_page = evict();
	  this->insertToFrame(free_page, buf, len);
	}
  }
}

void bufferPoolManager::printContent(int idx) {
  char *tmp = this->pages_[idx].content;
  int cnt = 0;
//    char *End = this->pages_[idx].content + sizeof(this->pages_[idx].content);
  while (tmp != this->pages_[idx].idx_ptr) {
	char *data_ptr = nullptr;
	std::memcpy(&data_ptr, tmp, sizeof(char *));
	char *next = nullptr;
	std::memcpy(&next, tmp + sizeof(char *), sizeof(char *));
	if (next == nullptr) {
	  next = this->pages_[idx].data_ptr;
	}
	size_t len = data_ptr - next;

	char data[len];
	std::memcpy(data, next, len);
	std::cout << data << std::endl;
	tmp += sizeof(char *);
	cnt += 1;
  }
}

bufferPoolManager::~bufferPoolManager() {
  for (auto cnt = 0; cnt < BUFFER_POOL_SIZE; cnt++) {
	if (this->is_dirty_[cnt]) {
	  this->stmgr_->writePage(this->page_table_[&this->pages_[cnt]], this->pages_[cnt].content);
	}
  }
}

heapPage *bufferPoolManager::findPage(PhysicalPageID idx) {
  for (auto it : this->page_table_) {
	if (it.second == idx) {
	  return it.first;
	}
  }
  return nullptr;
}

void bufferPoolManager::writeToDisk(PhysicalPageID psy_id, heapPage *page) {
  this->stmgr_->writePage(psy_id, page->content);
}

std::tuple<heapPage *, size_t> bufferPoolManager::nextFreePage() {
  size_t cnt = 0;
  for (auto it : this->page_table_) {
	if (it.second == INVALID_PHYSICAL_PAGE_ID) {
	  return {it.first, cnt};
	}
	++cnt;
  }
  return {nullptr, 0};
}
size_t bufferPoolManager::findPage(heapPage *target) {
  size_t cnt = 0;
  for (auto it : this->pages_) {
	if (it == *target) {
	  break;
	}
	++cnt;
  }
  return cnt;
}
void bufferPoolManager::allocateInMemoryView(RelID view_id) {
  heapPage *curPage;
  size_t cnt;
  std::tie(curPage, cnt) = this->nextFreePage();
  if (curPage == nullptr) {
	curPage = this->evict();
	cnt = this->findPage(curPage);
  }
  this->page_table_[curPage] = view_id;
  /* FIXME:
   * page table should be a map between heapPage & physical page id. But since PhysicalPageID starts from 0
   * and RelID is from std::time, this should be fine.
   */
}

heapPage::heapPage() {
  this->idx_ptr = (char *)(this->content);
  this->data_ptr = (char *)(this->content + HEAP_SIZE);
  this->timestamp = std::time(nullptr);
  std::memset(this->content, 0, HEAP_SIZE);
  this->id = this->timestamp;
  this->data_cnt = 0;
}

heapPage::~heapPage() {
//    std::free(this->content);
}

bool heapPage::insert(const char *buf, size_t len) {
  ++this->data_cnt;
  std::memcpy(this->idx_ptr, &(this->data_ptr), sizeof(char *));
  this->idx_ptr += sizeof(char *);
  this->data_ptr -= len;
  if (this->idx_ptr >= this->data_ptr) {
	return false;
  }
  std::memcpy(this->data_ptr, buf, len);
  return true;
}
inMemoryView::inMemoryView(bufferPoolManager *bpmgr) {
  this->id_ = std::time(nullptr);
  bpmgr->allocateInMemoryView(this->id_);
  this->pages_ = std::vector<heapPage *>(1, 0);
}
