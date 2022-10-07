//
// Created by scorp on 9/22/2022.
//
#include "executor.h"
#include "btree.h"

void executor::Init() {
  this->mode_ = volcano;
}

void scanExecutor::SetMode(execution_mode mode) {
  this->mode_ = mode;
}
void scanExecutor::SetTable(rel *tab) {
  this->table_ = tab;
}
void scanExecutor::SetBufferPoolManager(bufferPoolManager *manager) {
  this->bpmgr_ = manager;
}
void joinExecutor::Init() {
  executor::Init();
  this->left_child_ = nullptr; // wait for planner to set it
  this->right_child_ = nullptr; // wait for planner to set it
}
void createExecutor::Init() {
  executor::Init();
}
void createExecutor::setStorageManager(storageManager *manager) {
  this->storage_manager_ = manager;
}

void seqScanExecutor::Init(rel *tab, bufferPoolManager *manager, comparison_expr qual) {
  this->table_ = tab;
  this->bpmgr_ = manager;
  this->qual_ = qual;
  this->cnt_ = 0;
  this->pages_ = this->table_->get_location();
  if (manager->findPage(this->pages_[0]) == nullptr) {
	manager->readFromDisk(this->pages_[0]);
  }
  this->mem_ptr_ = manager->findPage(this->pages_[0])->content;
}
void seqScanExecutor::Next(void *dst) {
  size_t len = this->table_->get_tuple_size();
  if (this->mode_ == volcano) {
	// emit one at a time
	char buf[len];
	char *data_ptr = (char *)malloc(sizeof(char *));
	this->mem_ptr_ += sizeof(char *);
	std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
	if (data_ptr == nullptr) {
	  //last toyDBTUPLE
	  std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
	  data_ptr -= len;
	}
	std::memcpy(buf, data_ptr, len);
	toyDBTUPLE tmp(buf, len);
	((std::vector<toyDBTUPLE> *)dst)->push_back(tmp);
  } else {
	// emit a batch at a time
	for (auto i = 0; i < BATCH_SIZE; i++) {
	  char buf[len];
	  char *data_ptr = (char *)malloc(sizeof(char *));
	  this->mem_ptr_ += sizeof(char *);
	  std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
	  if (data_ptr == nullptr) {
		//last toyDBTUPLE
		std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
		data_ptr -= len;
	  }
	  std::memcpy(buf, data_ptr, len);
	  toyDBTUPLE tmp(buf, len);
	  ((std::vector<toyDBTUPLE> *)dst)->push_back(tmp);
	}
  }
}
void seqScanExecutor::End() {
//  free(this->memory_context_);
}
void indexExecutor::Init() {
  executor::Init();
}
void indexExecutor::Init(bufferPoolManager *bpmgr, rel *tab, size_t idx, index_type = btree) {
  this->bpmgr_ = bpmgr;
  PhysicalPageID idx_page = bpmgr->stmgr_->addPage(); // allocate a new page for index
  bTree<int> tree(10);
  //TODO: need a member in row/col to tell the datatype; also the spanning factor should be configurable
  if (tab->getStorageMethod() == row_store) {
	PhysicalPageID prev = INVALID_PHYSICAL_PAGE_ID;
	heapPage *cur_heap_page = nullptr;
	int cnt = 0;
	for (const auto &it : tab->rows_) {
	  if (it.pages_.size() == 1) {
		// every row is only spanned over 1 page, makes life much easier
		if (it.pages_[0] != prev) {
		  // need to fetch new page from disk. write previous page in buffer back.
		  if (prev != INVALID_PHYSICAL_PAGE_ID) {
			bpmgr->writeToDisk(prev, cur_heap_page);
		  }
		  cur_heap_page = bpmgr->findPage(it.pages_[0]);
		  if (cur_heap_page == nullptr) {
			// not in buffer pull
			bpmgr->readFromDisk(it.pages_[0]);
			cur_heap_page = bpmgr->findPage(it.pages_[0]);
		  }
		  cnt = 0;
		  prev = it.pages_[0];
		}
		char *data_ptr = (char *)malloc(sizeof(char *));
		std::memcpy(&data_ptr, cur_heap_page->content + idx * (sizeof(char *)), sizeof(char *));
		char *prev_data_ptr = (char *)malloc(sizeof(char *));
		std::memcpy(&prev_data_ptr, cur_heap_page->content + (idx - 1) * (sizeof(char *)), sizeof(char *));
		size_t len = prev_data_ptr - data_ptr;
		char *val = (char *)std::malloc(len);
		std::memcpy(val, data_ptr, len);
		tree.insert(*val, std::tuple(prev, cnt));
	  }
	}
  }
  bpmgr->stmgr_->writePage(idx_page, &tree);
}
