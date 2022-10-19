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
#include "schema.h"

void executor::Init() {
  std::memset(this->mem_context_, 0, EXEC_MEM);
  this->mode_ = volcano;
  this->free_spaces_[this->mem_context_] = EXEC_MEM;
  this->view_ = new rel;
}
char *executor::talloc(size_t size) {
  for (auto it : this->free_spaces_) {
	if (it.second >= size) {
	  this->free_spaces_.erase(it.first);
	  this->allocated_spaces[it.first] = size;
	  if (it.second != size) {
		this->free_spaces_.insert({it.first + size, it.second - size});
	  }
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
	this->free_spaces_.erase(it->first);
	this->free_spaces_[it->first - size] = it->second + size;
  }
  this->free_spaces_.insert({dst, size});
}
void executor::tfree(char *dst) {
  auto it = this->allocated_spaces.find(dst);
  if (it == this->allocated_spaces.cend()) {
	std::cout << "Trying to free a memory chunk not allocated or not in executor's context." << std::endl;
	return;
  } else {
	this->tfree(dst, this->allocated_spaces[dst]);
  }
}
void executor::End() {
  PhysicalPageID tmp = this->bpmgr_->stmgr_->addPage();
  this->bpmgr_->stmgr_->writePage(tmp, this->mem_context_);
  this->bpmgr_;
//  free(this->mem_context_);
}

void scanExecutor::SetMode(execution_mode mode) {
  this->mode_ = mode;
}
void scanExecutor::SetTable(rel *tab) {
  this->table_ = tab;
}
void executor::SetBufferPoolManager(bufferPoolManager *manager) {
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
  std::string view_name = "Seq Scan View for Tab " + tab->GetName();
  this->view_->SetName(view_name);
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
//  size_t col_idx = std::get<1>(this->qual_->data_srcs[0]);
  size_t offset = this->table_->GetOffset(std::get<3>(this->qual_->data_srcs[0]));
  char *rhs = (char *)std::get<3>(this->qual_->data_srcs[1]).c_str();
  if (this->mode_ == volcano) {
	// emit one at a time
	char *buf = talloc(len);
	std::memset(buf, 0, len);
	char *data_ptr = talloc(sizeof(char *));
	this->mem_ptr_ += sizeof(char *);
	if (std::memcmp(this->mem_ptr_, buf, sizeof(int))
		== 0) { // Little trick: currently buf is an empty char array with min. 4 bytes.
	  //last toyDBTUPLE
	  std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
	  if (data_ptr == nullptr) {
		// One past the last toyDBTuple
		// write an empty tuple to dst
		toyDBTUPLE tmp;
		((std::vector<toyDBTUPLE> *)dst)->at(0) = tmp;
		tfree(buf);
		return;
	  } else {
		data_ptr -= len;
	  }
	} else {
	  std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
	}
	std::memcpy(buf, data_ptr, len);
	auto type_ids = this->table_->GetTypeIDs();
	toyDBTUPLE tmp((char *)buf, len, sizes, type_ids);
	tmp.table_ = std::get<0>(table_schema.Table2IDName[this->table_]);
	// Verify comparison expression
	if (this->qual_ == nullptr || this->qual_->type == COL
		|| this->qual_->compareFunc((char *)buf + offset, (char *)rhs)) {
	  ((std::vector<toyDBTUPLE> *)dst)->at(0) = tmp;
	  ++this->cnt_;
	}
	//	((std::vector<toyDBTUPLE> *)dst)->at(0) = *tmp;
	//	((std::vector<toyDBTUPLE> *)dst)->emplace(((std::vector<toyDBTUPLE> *)dst)->begin(), (char *)buf, len, sizes);
	tfree(buf);
	//	tfree(data_ptr);
	//	((std::vector<toyDBTUPLE> *)dst)->emplace_back((char *)buf, len, sizes);
  } else {
	// emit a batch at a time TODO: predicate validation
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
	  ((std::vector<toyDBTUPLE> *)dst)->emplace_back((char *)buf, len, sizes, this->table_->GetTypeIDs());
	  tfree(buf, len * BATCH_SIZE);
	  tfree(data_ptr, sizeof(char *));
	}
  }
}
void seqScanExecutor::End() {
  executor::End();
//  free(this->mem_context_);
}
void seqScanExecutor::Reset() {
  // In case the first page is already evicted.
  this->mem_ptr_ = this->bpmgr_->findPage(this->pages_[0])->content;
  if (this->mem_ptr_ == nullptr) {
	this->bpmgr_->readFromDisk(this->pages_[0]);
	this->mem_ptr_ = this->bpmgr_->findPage(this->pages_[0])->content;
  }
  this->cnt_ = 0;
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
		char *data_ptr = talloc(sizeof(char *));
		std::memcpy(&data_ptr, cur_heap_page->content + idx * (sizeof(char *)), sizeof(char *));
		char *prev_data_ptr = talloc(sizeof(char *));
		std::memcpy(&prev_data_ptr, cur_heap_page->content + (idx - 1) * (sizeof(char *)), sizeof(char *));
		size_t len = prev_data_ptr - data_ptr;
		char *val = talloc(len);
		std::memcpy(val, data_ptr, len);
		tree.insert(*val, std::tuple(prev, cnt));
		tfree(data_ptr);
		tfree(prev_data_ptr);
		tfree(val);
	  }
	}
  }
  bpmgr->stmgr_->writePage(idx_page, &tree);
}
void selectExecutor::Init(std::vector<expr *> exprs, std::vector<executor *> children) {
  executor::Init();
  this->targetList_ = exprs;
  this->children_ = std::move(children);
}
void selectExecutor::Next(void *dst) {
  if (this->cnt_ == 0){
	std::cout << "    ";
	for (auto it : this->targetList_) {
	  std::cout << "|" << it->alias;
	}
	std::cout << std::endl;
  }
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
		if (col_size == 0) {
		  // skip the first empty entry
		  continue;
		}
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
void selectExecutor::Init() {
  executor::Init();
}
void nestedLoopJoinExecutor::Init() {
  executor::Init();
  this->curLeftBatch_ = std::vector<toyDBTUPLE>(BATCH_SIZE);
  this->curRightBatch_ = std::vector<toyDBTUPLE>(BATCH_SIZE);
  this->left_child_->Next(&this->curLeftBatch_);
  this->curLeftTuple_ = this->curLeftBatch_.begin();
  this->right_child_->Next(&this->curRightBatch_);
  this->curRightTuple_ = this->curRightBatch_.begin();
}
void nestedLoopJoinExecutor::Next(void *dst) {
  size_t cnt = 0;
  for (;;) {
	// for every left tuple, iterate thru all right tuples, stops when BATCH_SIZE of tuples found or all scanned.

	//find offset of column content for predicate validation
	std::string
		col_name = std::get<3>(this->pred_->data_srcs[0]).substr(std::get<3>(this->pred_->data_srcs[0]).find('.') + 1);
	rel *l_tab = table_schema.TableID2Table[this->curLeftTuple_->table_];
	std::vector<size_t> sizes = l_tab->GetColSizes();
	size_t l_col_offset = l_tab->GetOffset(col_name);
//	size_t l_col_idx = l_tab->GetColIdx(col_name);

	// Iterate thru all right tuples (start from the current right tuple)
	rel *r_tab = table_schema.TableID2Table[this->curLeftTuple_->table_];
	std::vector<size_t> r_sizes = r_tab->GetColSizes();
	size_t r_col_offset = r_tab->GetOffset(col_name);
//		  std::cout << "Comparing " << (int)*(left.content_ + l_col_offset) << " from left side with "
//					<< (int)*(right.content_ + r_col_offset) << " from right side" << std::endl;
	if (this->pred_ == nullptr || this->pred_->compareFunc(this->curLeftTuple_->content_ + l_col_offset,
														   this->curRightTuple_->content_ + r_col_offset)) {
	  // pass predicate, join this two tuple tgt
	  toyDBTUPLE *tup = this->Join(this->curLeftTuple_.base(), this->curRightTuple_.base(), col_name);
	  ((std::vector<toyDBTUPLE> *)dst)->at(cnt) = *tup;
	  ++cnt;
	}
	// Finished evaluating curRightTuple and curLeftTuple, move to the next pair
	this->curRightTuple_ = std::next(this->curRightTuple_);
	if (this->curRightTuple_ == curRightBatch_.end()) {
	  // Current batch of right tuples all validated, retrieve the next batch
	  this->right_child_->Next(&this->curRightBatch_);
	  if (this->curRightBatch_.begin()->content_ == nullptr) {
		// iterate thru all right tuples already, get to the next left tuple
		((seqScanExecutor *)
			this->right_child_)->Reset(); // TODO: this should be compatible any other executors.
		this->curLeftTuple_ = std::next(this->curLeftTuple_);
		if (this->curLeftTuple_ == this->curLeftBatch_.end()) {
		  // Reached end of current block, retrieve the next block
		  this->left_child_->Next(&this->curLeftBatch_);
		  if (this->curLeftBatch_.begin()->content_ == nullptr) {
			// iterate thru all left & right tuples, just return
			return;
		  }
		  this->curLeftTuple_ = this->curLeftBatch_.begin();
		}
		this->right_child_->Next(&this->curRightBatch_);
	  }
	  this->curRightTuple_ = this->curRightBatch_.begin();
	}

	if (cnt == BATCH_SIZE) {
	  return;
	}
  }
}
void nestedLoopJoinExecutor::End() {
  executor::End();
  this->left_child_->End();
  this->right_child_->End();
}
toyDBTUPLE *joinExecutor::Join(toyDBTUPLE *left, toyDBTUPLE *right, const std::string &col_name) {
  std::vector<size_t> l_sizes = left->sizes_;
  std::vector<size_t> l_types = left->type_ids_;
  rel *r_tab = table_schema.TableID2Table[right->table_];
  size_t r_idx = r_tab->GetColIdx(col_name);
  std::vector<size_t> r_sizes = right->sizes_;
  size_t ttl_size = left->size_ + right->size_ - right->sizes_[r_idx];
  char *buf = talloc(ttl_size);
  std::memcpy(buf, left->content_, left->size_);
  size_t offset_f = 0, offset_r = 0;
  for (size_t cnt = 0; cnt < right->sizes_.size(); ++cnt) {
	if (this->pred_->comparision_type != equal or cnt != r_idx) {
	  std::memcpy(buf + left->size_ + offset_f, right->content_ + offset_r, r_sizes[cnt]);
	  offset_f += r_sizes[cnt];
	  l_sizes.push_back(r_sizes[cnt]);
	  l_types.push_back(right->type_ids_[cnt]);
	}
	offset_r += r_sizes[cnt];
  }
  auto *out = (toyDBTUPLE *)talloc(sizeof(toyDBTUPLE));
  out->content_ = buf;
  out->sizes_ = l_sizes;
  out->size_ = ttl_size;
  out->type_ids_ = l_types;
  return out;
}
