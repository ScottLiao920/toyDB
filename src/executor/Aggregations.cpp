// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/24/2022.
//

#include "executor.h"
#include "schema.h"
#include "type.h"

void aggregateExecutor::Init() {
  executor::Init();
  this->child_->Init();
  std::memset(this->result_, 0, 64);
}
void aggregateExecutor::SetColumn(std::string inp) {
  // View is not initialized until Init()
  this->column_name_ = std::move(inp);
//  this->col_idx =
//	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(inp);
//  this->col_offset =
//	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(inp);
}
void aggregateExecutor::End() {
  executor::End();
}
void SumAggregateExecutor::Next(void *dst) {
  this->col_idx =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(this->column_name_);
  this->col_offset =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(this->column_name_);
  while (true) {
	std::vector<toyDBTUPLE> child_output(BATCH_SIZE);
	this->child_->Next(&child_output);
	if (child_output.empty() || child_output.at(0).content_ == nullptr) {
	  break;
	}
	// Here should be a loop iterate thru all emitted tuples
	for (auto it : child_output) {
	  switch (type_schema.typeID2type[it.type_ids_[col_idx]]) {
		case (1): {
		  auto tmp = (long long)*result_;
		  tmp += (int)(*it.content_ + col_offset);
		  std::memcpy(result_, &tmp, sizeof(long long));
		  break;
		}
		case (2): {
		  auto tmp = (double)*result_;
		  tmp += (double)(*it.content_ + col_offset);
		  std::memcpy(result_, &tmp, sizeof(double));
		  break;
		}
		case (3): {
		  auto tmp = (unsigned long long)*result_;
		  tmp += (unsigned long long)(*it.content_ + col_offset);
		  std::memcpy(result_, &tmp, sizeof(unsigned long long));
		  break;
		}
		default: break;
	  }
	}
  }
//  toyDBTUPLE tmp;
  toyDBTUPLE tmp((char *)this->result_, 64, {64}, {1}); // Fixme
  tmp.ancestor_ = 1; // Fixme
  tmp.table_ = this->view_->GetID();

  (std::vector<toyDBTUPLE> *)dst;
  std::memcpy(dst, this->result_, 64);
}
void CountAggregateExecutor::Next(void *dst) {
  this->col_idx =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(this->column_name_);
  this->col_offset =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(this->column_name_);
  size_t cnt = 0;
  while (true) {
	std::vector<toyDBTUPLE> child_output(BATCH_SIZE);
	this->child_->Next(&child_output);
	if (child_output.empty() || child_output.at(0).content_ == nullptr) {
	  break;
	}
	cnt += child_output.size();
  }
  std::memcpy(this->result_, &cnt, sizeof(size_t));
  std::memcpy(dst, this->result_, 64);
}
void MeanAggregateExecutor::Next(void *dst) {
  this->col_idx =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(this->column_name_);
  this->col_offset =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(this->column_name_);
  size_t cnt = 0;
  size_t type_idx = 1;
  while (true) {
	std::vector<toyDBTUPLE> child_output(BATCH_SIZE);
	this->child_->Next(&child_output);
	if (child_output.empty() || child_output.at(0).content_ == nullptr) {
	  break;
	}
	// Here should be a loop iterate thru all emitted tuples
	for (auto it : child_output) {
	  ++cnt;
	  switch (type_schema.typeID2type[it.type_ids_[col_idx]]) {
		case (1): {
		  auto tmp = (long long)*result_;
		  tmp += (int)(*it.content_ + col_offset);
		  std::memcpy(result_, &tmp, sizeof(long long));
		  type_idx = 1;
		  break;
		}
		case (2): {
		  auto tmp = (double)*result_;
		  tmp += (double)(*it.content_ + col_offset);
		  std::memcpy(result_, &tmp, sizeof(double));
		  type_idx = 2;
		  break;
		}
		case (3): {
		  auto tmp = (unsigned long long)*result_;
		  tmp += (unsigned long long)(*it.content_ + col_offset);
		  std::memcpy(result_, &tmp, sizeof(unsigned long long));
		  type_idx = 3;
		  break;
		}
		default: break;
	  }
	}
  }
  switch (type_idx) {
	case (1): {
	  auto tmp = (long long)*this->result_;
	  tmp /= cnt;
	  std::memcpy(this->result_, &tmp, sizeof(long long));
	  break;
	}
	case (2): {
	  auto tmp = (double)*this->result_;
	  tmp /= (double)cnt;
	  std::memcpy(this->result_, &tmp, sizeof(double));
	  break;
	}
	case (3): {
	  auto tmp = (unsigned long long)*this->result_;
	  tmp /= cnt;
	  std::memcpy(this->result_, &tmp, sizeof(unsigned long long));
	  break;
	}
  }
  std::memcpy(dst, this->result_, 64);
}