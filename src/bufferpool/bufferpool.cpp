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
  this->is_dirty_.reserve(this->no_pages_);
  this->pin_cnt_.reserve(this->no_pages_);
}

heapPage *bufferPoolManager::evict() {
  // LRU replacement
  std::time_t oldest = this->pages_.cbegin()->timestamp;
  unsigned int cnt;
  for (cnt = 0; cnt < this->no_pages_; ++cnt) {
    if (this->pin_cnt_[cnt] == 0) {
      if (this->pages_[cnt].timestamp < oldest) {
        if (this->is_dirty_[cnt]) {
          // write dirty page to disk before evict it;
          this->writeToDisk(this->page_table_[&this->pages_[cnt]], cnt);
        }
        std::memset(this->pages_[cnt].content, 0, HEAP_SIZE);
      }
    }
  }
  return &this->pages_[cnt];
}

void bufferPoolManager::readFromDisk(PhysicalPageID psy_id) {
  int cnt = 0;
  for (auto &it : this->page_table_) {
    if (it.second == INVALID_PHYSICAL_PAGE_ID) {
      it.second = psy_id;
      this->stmgr_->readPage(psy_id, it.first->content);
      it.first->timestamp = std::time(nullptr);
      it.first->data_cnt = this->stmgr_->getDataCnt(psy_id);
      it.first->data_ptr = it.first->content + HEAP_SIZE;
      it.first->idx_ptr = it.first->content;
      this->pin_cnt_[cnt] += 1;
      this->is_dirty_[cnt] = false;
      cnt += 1;
      return;
    }
  }
  heapPage *cleanedPage = evict();
  this->stmgr_->readPage(psy_id, cleanedPage->content);
  cleanedPage->data_cnt = this->stmgr_->getDataCnt(psy_id);
  this->page_table_[cleanedPage] = psy_id;
  cleanedPage->timestamp = std::time(nullptr);
}

void bufferPoolManager::writeToDisk(PhysicalPageID psy_id, int idx) {
  this->stmgr_->writePage(psy_id, this->pages_[idx].content);
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
//    char *end = this->pages_[idx].content + sizeof(this->pages_[idx].content);
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

heapPage::heapPage() {
  this->idx_ptr = (char *) (this->content);
  this->data_ptr = (char *) (this->content + sizeof(content));
  this->timestamp = std::time(nullptr);
  std::memset(this->content, 0, HEAP_SIZE);
  this->id = this->timestamp;
  this->data_cnt = 0;
}

heapPage::~heapPage() {
//    std::free(&this->content);
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
