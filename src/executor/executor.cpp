// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 9/22/2022.
//
#include "executor.h"

#include <utility>
#include "btree.h"
#include "common.h"

void executor::Init() {
  std::memset(this->mem_context_, 0, EXEC_MEM);
  this->mode_ = volcano;
  this->free_spaces_[this->mem_context_] = EXEC_MEM;
}
char *executor::talloc(size_t size) {
  for (auto it : this->free_spaces_) {
	if (it.second >= size) {
	  this->free_spaces_.erase(it.first);
	  this->free_spaces_.insert({it.first + size, it.second - size});
	  return it.first;
	}
  }
  return nullptr;
}
void executor::tfree(char *dst, size_t size) {
  if (dst < this->mem_context_ || dst > this->mem_context_ + EXEC_MEM) {
	std::cout << "Trying to free a memory chunk not in executor's context." << std::endl;
	return;
  }
  auto tmp = this->allocated_spaces.find(dst);
  if (tmp != this->allocated_spaces.cend()) {
	this->allocated_spaces.erase(tmp);
  } else {
	std::cout << "Trying to free a memory chunk not allocated." << std::endl;
  }
  std::memset(dst, 0, size);
  auto it = this->free_spaces_.find(dst + size);
  if (it != this->free_spaces_.cend()) {
	// There's a consecutive free memory chunk, update it to (dst, size+prev_size)
	auto selected_chunk = this->free_spaces_.extract(it->first);
	selected_chunk.key() = it->first - size;
	this->free_spaces_.insert(std::move(selected_chunk));
	this->free_spaces_[it->first - size] = it->second + size;
  }
  this->free_spaces_.insert({dst, size});
}
void executor::tfree(char *dst) {
  auto it = this->allocated_spaces.find(dst);
  if (it != this->allocated_spaces.cend()) {
	std::cout << "Trying to free a memory chunk not allocated or not in executor's context." << std::endl;
	return;
  } else {
	this->tfree(dst, this->allocated_spaces[dst]);
  }
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
void createExecutor::Init() {
  executor::Init();
}
void createExecutor::setStorageManager(storageManager *manager) {
  this->storage_manager_ = manager;
}

void seqScanExecutor::Init(rel *tab, bufferPoolManager *manager, comparison_expr *qual) {
  executor::Init();
  this->table_ = tab;
  this->bpmgr_ = manager;
  this->view_->SetName("Seq Scan View for Tab " + tab->GetName());
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
  std::vector<size_t> sizes = this->table_->GetColSizes();
  if (this->mode_ == volcano) {
	// emit one at a time
	char *buf = talloc(len);
	std::memset(buf, 0, len);
	char *data_ptr = (char *)malloc(sizeof(char *));
	this->mem_ptr_ += sizeof(char *);
	std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
	if (data_ptr == nullptr) {
	  //last toyDBTUPLE
	  std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
	  data_ptr -= len;
	}
	std::memcpy(buf, data_ptr, len);
//	auto tmp = new toyDBTUPLE((char *)buf, len, sizes);
//	((std::vector<toyDBTUPLE> *)dst)->at(0) = *tmp;
	((std::vector<toyDBTUPLE> *)dst)->emplace(((std::vector<toyDBTUPLE> *)dst)->begin(), (char *)buf, len, sizes);
	tfree(buf, len);
//	((std::vector<toyDBTUPLE> *)dst)->emplace_back((char *)buf, len, sizes);
  } else {
	// emit a batch at a time
	for (auto i = 0; i < BATCH_SIZE; i++) {
	  char *buf = talloc(len * BATCH_SIZE);
	  char *data_ptr = talloc(sizeof(char *));
	  this->mem_ptr_ += sizeof(char *);
	  std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
	  if (data_ptr == nullptr) {
		//last toyDBTUPLE
		std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
		data_ptr -= len;
	  }
	  std::memcpy(buf, data_ptr, len);
	  ((std::vector<toyDBTUPLE> *)dst)->emplace_back((char *)buf, len, sizes);
	  tfree(buf, len * BATCH_SIZE);
	  tfree(data_ptr, sizeof(char *));
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
  executor::Init();
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
void selectExecutor::Init(std::vector<expr *> exprs, std::vector<executor *> children) {
  executor::Init();
  this->targetList_ = exprs;
  this->children_ = std::move(children);
  std::cout << "    ";
  for (auto it : exprs) {
	std::cout << "|" << it->alias;
  }
  std::cout << std::endl;
}
void selectExecutor::Next(void *dst) {
  std::cout << "    ";
  for (auto it : children_) {
	auto *buf = new std::vector<toyDBTUPLE>;
	it->Next(buf);
	if (buf->empty()) {
	  std::memset(dst, 0, sizeof(char));
	  continue;
	}
	if (this->mode_ == volcano) {
	  size_t offset = 0;
	  for (auto col_size : buf->cbegin()->sizes_) {
		// TODO: validate targetList on tmp_buf here
		char tmp_buf[col_size];
		std::memcpy(tmp_buf, buf->cbegin()->content_ + offset, col_size);
		switch (1) {//type_schema.typeID2type[table1.cols_[col_id].typeid_]) {
		  case (1): {
			std::cout << "|" << (int)*tmp_buf;
			break;
		  }
		  case (2): {
			std::cout << "|" << (float)*tmp_buf;
			break;
		  }
		  case (3): {
			std::cout << "|" << (size_t)*tmp_buf;
			break;
		  }
		  case (4): {
			std::cout << "|" << *tmp_buf;
			break;
		  }
		}
		offset += col_size;
	  }
	} else {
	  //TODO: For batched execution
	}
	delete buf;
  }
  std::cout << std::endl;
  this->cnt_ += 1;
}
void selectExecutor::End() {
  std::cout << "Output " << this->cnt_ << "tuples." << std::endl;
}
void nestedLoopJoinExecutor::Next(void *dst) {
  for (;;) {
	std::vector<toyDBTUPLE> left_tuple;
	this->left_child_->Next(&left_tuple);
	if (left_tuple.empty()) {
	  break;
	}
	for (const auto &it : left_tuple) {
	  std::vector<toyDBTUPLE> right_tuple;
	  this->right_child_->Next(&right_tuple);
	  if (right_tuple.empty()) {
		break;
	  }
	  for (const auto &right : right_tuple) {
//		if (this->pred_->compare(it.content_, right.content_)) {
//		  ((std::vector<toyDBTUPLE> *)dst)->push_back(it, right);
//		}
	  }
	}
  }
}
