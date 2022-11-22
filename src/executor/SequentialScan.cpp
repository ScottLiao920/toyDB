// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/20/2022.
//
#include "executor.h"

void seqScanExecutor::Init() {
  executor::Init();
  this->view_->SetName("Seq Scan View for Tab " + this->table_->GetName());
  this->view_->cols_ = this->table_->cols_; // Scan executors are leaf executors so no need to set column sources again.
  this->cnt_ = 0;
  this->pages_ = this->table_->GetLocation();
  if (this->bpmgr_->findPage(this->pages_[0]) == nullptr) {
	this->bpmgr_->readFromDisk(this->pages_[0]);
  }
  this->mem_ptr_ = this->bpmgr_->findPage(this->pages_[0])->content;
}
void seqScanExecutor::Init(rel *tab, bufferPoolManager *manager, comparison_expr *qual) {
  executor::Init();
  this->table_ = tab;
  this->bpmgr_ = manager;
  this->view_->SetName("Seq Scan View for Tab " + tab->GetName());
  this->view_->cols_ = tab->cols_; // Scan executors are leaf executors so no need to set column sources again.
  this->quals_.emplace_back(qual);
  this->cnt_ = 0;
  this->pages_ = this->table_->GetLocation();
  if (manager->findPage(this->pages_[0]) == nullptr) {
	manager->readFromDisk(this->pages_[0]);
  }
  this->mem_ptr_ = manager->findPage(this->pages_[0])->content;
}
void seqScanExecutor::Next(void *dst) {
  //Iterate until a tuple matched predicate or all tuples emitted.
  while (true) {
	size_t len = this->table_->GetTupleSize();
	std::vector<size_t> sizes = this->table_->GetColSizes();
//  size_t col_idx = std::get<1>(this->quals_->data_srcs[0]);
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
		  // clear dst vector
		  ((std::vector<toyDBTUPLE> *)dst)->clear();
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
	  tmp.ancestor_ = this->table_->GetID();
	  tmp.table_ = this->view_->GetID();
	  // Verify predicates
	  bool matched = true;
	  for (auto qual : this->quals_) {
		size_t offset = this->table_->GetOffset(std::get<3>(qual->data_srcs[0]));
		char *rhs = (char *)std::get<3>(qual->data_srcs[1]).c_str();
		if (qual->type != COL && !qual->compareFunc((char *)buf + offset, (char *)rhs)) {
		  matched = false;
		  break;
		}
	  }
	  if (matched) {
		((std::vector<toyDBTUPLE> *)dst)->at(0) = tmp;
		++this->cnt_;
		break;
	  }
	  //	((std::vector<toyDBTUPLE> *)dst)->at(0) = *tmp;
	  //	((std::vector<toyDBTUPLE> *)dst)->emplace(((std::vector<toyDBTUPLE> *)dst)->begin(), (char *)buf, len, sizes);
	  tfree(buf);
	  //	tfree(data_ptr);
	  //	((std::vector<toyDBTUPLE> *)dst)->emplace_back((char *)buf, len, sizes);
	} else {
	  // emit a batch at a time
	  // TODO: currently still a loop of volcano model. need to implement vectorized storage&predicate validation.
	  size_t cnt = 0;
	  char *buf = talloc(len * BATCH_SIZE);
	  std::memset(buf, 0, len * BATCH_SIZE);
	  while (cnt < BATCH_SIZE) {
		char *data_ptr = talloc(sizeof(char *));
		this->mem_ptr_ += sizeof(char *);
		if (std::memcmp(this->mem_ptr_, buf, sizeof(int))
			== 0) { // Little trick: currently buf is an empty char array with min. 4 bytes.
		  //last toyDBTUPLE
		  std::memcpy(&data_ptr, this->mem_ptr_ - sizeof(char *), sizeof(char *));
		  if (data_ptr == nullptr) {
			// One past the last toyDBTuple
			// clear dst vector
//			((std::vector<toyDBTUPLE> *)dst)->clear();
//			tfree(buf);
			return;
		  } else {
			data_ptr -= len;
		  }
		} else {
		  std::memcpy(&data_ptr, this->mem_ptr_, sizeof(char *));
		}
		std::memcpy(buf + cnt * len, data_ptr, len);
		auto type_ids = this->table_->GetTypeIDs();
		toyDBTUPLE tmp((char *)buf, len, sizes, type_ids);
		tmp.ancestor_ = this->table_->GetID();
		tmp.table_ = this->view_->GetID();
		// Verify predicates
		bool matched = true;
		for (auto qual : this->quals_) {
		  size_t offset = this->table_->GetOffset(std::get<3>(qual->data_srcs[0]));
		  char *rhs = (char *)std::get<3>(qual->data_srcs[1]).c_str();
		  if (qual->type != COL && !qual->compareFunc((char *)buf + offset, (char *)rhs)) {
			matched = false;
			break;
		  }
		}
		if (matched) {
		  ((std::vector<toyDBTUPLE> *)dst)->at(cnt) = tmp;
		  ++this->cnt_;
		  ++cnt;
		  break;
		}
	  }
	  tfree(buf);
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