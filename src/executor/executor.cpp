//
// Created by scorp on 9/22/2022.
//
#include "executor.h"
#include "btree.h"

void executor::Init() {
  std::memset(this->memory_context_, 0, EXEC_MEM);
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
  manager->stmgr_->readPage(this->pages_[0], this->memory_context_);
}
std::vector<tuple> seqScanExecutor::Next() {
  std::vector<tuple> out;
  size_t len = this->table_->get_tuple_size();
  if (this->mode_ == volcano) {
	// emit one at a time
	char buf[len];
	char *data_ptr = (char *)malloc(sizeof(char *));
	this->mem_ptr_ += sizeof(char *);
	std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
	if (data_ptr == nullptr) {
	  //last tuple
	  std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
	  data_ptr -= len;
	}
	std::memcpy(buf, data_ptr, len);
	tuple tmp(buf, len);
	out.push_back(tmp);
  } else {
	// emit a batch at a time
	for (auto i = 0; i < BATCH_SIZE; i++) {
	  char buf[len];
	  std::memcpy(buf, this->mem_ptr_, len);
	  tuple tmp(buf, len);
	  out.push_back(tmp);
	  this->mem_ptr_ += len;
	}
  }
  return out;
}
void seqScanExecutor::End() {
//  free(this->memory_context_);
}
void indexExecutor::Init() {
  executor::Init();
}
void indexExecutor::Init(storageManager *stmgr, rel *tab, size_t idx, index_type type = btree) {
  PhysicalPageID idx_page = stmgr->addPage();
  bTree<int> tree(10); // need a member in row/col to tell the datatype
  if (tab->getStorageMethod() == row_store) {
	PhysicalPageID prev = INVALID_PHYSICAL_PAGE_ID;
	char buf[PHYSICAL_PAGE_SIZE];
	char *ptr = buf;
	int cnt = 0;
	for (const auto &it : tab->rows_) {
	  if (it.pages_.size() == 1) {
		// every row is only spanned over 1 page, makes life much easier
		if (it.pages_[0] != prev) {
		  // write previous page in buffer back, read new page from disk
		  if (prev != INVALID_PHYSICAL_PAGE_ID) {
			stmgr->writePage(prev, buf);
		  }
		  stmgr->readPage(it.pages_[0], buf);
		  ptr = buf;
		  cnt = 0;
		  prev = it.pages_[0];
		}
		int *val = nullptr;
		std::memcpy(val, ptr, sizeof(int)); // not true. Need to be the idx-th column
		tree.insert(*val, std::tuple(prev, cnt));
	  }
	}
  }
  stmgr->writePage(idx_page, &tree);
}
