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
//  this->column_name_ = this->column_name_.substr(this->column_name_.find('.') + 1);
//  this->col_idx =
//	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(inp);
//  this->col_offset =
//	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(inp);
}
void SumAggregateExecutor::Init() {
  aggregateExecutor::Init();
  this->view_->SetName("Sum Aggregation for Tab {" + this->child_->GetViewName() + "}");
  for (auto it : table_schema.TableID2Table[this->child_->GetViewID()]->cols_) {
	std::string upper_name = it.getName();
	std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
	if (upper_name.find(this->column_name_) != std::string::npos) {
	  this->view_->cols_.push_back(it);
	  break;
	}
  }
}
void aggregateExecutor::End() {
  executor::End();
}
void SumAggregateExecutor::Next(void *dst) {
  if (this->finished_) {
	((std::vector<toyDBTUPLE> *)dst)->clear();
	((std::vector<toyDBTUPLE> *)dst)->resize(BATCH_SIZE);
	return;
  }
  this->col_idx =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(this->column_name_);
  this->col_offset =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(this->column_name_);
  size_t type_id;
  RelID cur_ancestor;
  while (true) {
	std::vector<toyDBTUPLE> child_output(BATCH_SIZE);
	this->child_->Next(&child_output);
	if (child_output.empty() || child_output.at(0).content_ == nullptr) {
	  ((std::vector<toyDBTUPLE> *)dst)->clear();
	  ((std::vector<toyDBTUPLE> *)dst)->resize(BATCH_SIZE);
	  break;
	}
	cur_ancestor = child_output.at(0).ancestor_;
	// Here should be a loop iterate thru all emitted tuples
	for (auto it : child_output) {
	  type_id = type_schema.typeID2type[it.type_ids_[col_idx]];
	  switch (type_schema.typeID2type[it.type_ids_[col_idx]]) {
		case (1): {
		  auto tmp = (long long *)result_;
		  *tmp += (int)(*(it.content_ + col_offset));
		  std::memcpy(result_, tmp, sizeof(long long));
		  break;
		}
		case (2): {
		  auto tmp = (double *)result_;
		  *tmp += (double)(*(it.content_ + col_offset));
		  std::memcpy(result_, tmp, sizeof(double));
		  break;
		}
		case (3): {
		  auto tmp = (unsigned long long *)result_;
		  *tmp += (unsigned long long)(*(it.content_ + col_offset));
		  std::memcpy(result_, tmp, sizeof(unsigned long long));
		  break;
		}
		default: break;
	  }
	}
  }
  toyDBTUPLE tmp((char *)this->result_, 64, {64}, {type_id});
  tmp.ancestor_ = cur_ancestor;
  tmp.table_ = this->view_->GetID();

  ((std::vector<toyDBTUPLE> *)dst)->at(0) = tmp;
  this->finished_ = true;
}
void CountAggregateExecutor::Init() {
  aggregateExecutor::Init();
  this->view_->SetName("Count Aggregation for Tab {" + this->child_->GetViewName() + "}");
  for (auto it : table_schema.TableID2Table[this->child_->GetViewID()]->cols_) {
	std::string upper_name = it.getName();
	std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
	if (upper_name.find(this->column_name_) != std::string::npos) {
	  this->view_->cols_.push_back(it);
	  break;
	}
  }
}
void CountAggregateExecutor::Next(void *dst) {
  if (this->finished_) {
	((std::vector<toyDBTUPLE> *)dst)->clear();
	((std::vector<toyDBTUPLE> *)dst)->resize(BATCH_SIZE);
	return;
  }
  this->col_idx =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(this->column_name_);
  this->col_offset =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(this->column_name_);
  size_t cnt = 0;
  size_t cur_ancestor;
  while (true) {
	std::vector<toyDBTUPLE> child_output(BATCH_SIZE);
	this->child_->Next(&child_output);
	if (child_output.empty() || child_output.at(0).content_ == nullptr) {
	  ((std::vector<toyDBTUPLE> *)dst)->clear();
	  ((std::vector<toyDBTUPLE> *)dst)->resize(BATCH_SIZE);
	  break;
	}
	cur_ancestor = child_output.at(0).ancestor_;
	cnt += child_output.size();
  }
  std::memcpy(this->result_, &cnt, sizeof(size_t));
  toyDBTUPLE tmp((char *)this->result_, 64, {64}, {typeid(size_t).hash_code()});
  tmp.ancestor_ = cur_ancestor;
  tmp.table_ = this->view_->GetID();

  ((std::vector<toyDBTUPLE> *)dst)->at(0) = tmp;
  this->finished_ = true;
}
void MeanAggregateExecutor::Init() {
  aggregateExecutor::Init();
  this->view_->SetName("Mean Aggregation for Tab {" + this->child_->GetViewName() + "}");
  for (auto it : table_schema.TableID2Table[this->child_->GetViewID()]->cols_) {
	std::string upper_name = it.getName();
	std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
	if (upper_name.find(this->column_name_) != std::string::npos) {
	  this->view_->cols_.push_back(it);
	  break;
	}
  }
}
void MeanAggregateExecutor::Next(void *dst) {
  if (this->finished_) {
	((std::vector<toyDBTUPLE> *)dst)->clear();
	((std::vector<toyDBTUPLE> *)dst)->resize(BATCH_SIZE);
	return;
  }
  this->col_idx =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetColIdx(this->column_name_);
  this->col_offset =
	  table_schema.TableID2Table[this->child_->GetViewID()]->GetOffset(this->column_name_);
  size_t cnt = 0;
  size_t type_idx = 1;
  RelID cur_ancestor;
  while (true) {
	std::vector<toyDBTUPLE> child_output(BATCH_SIZE);
	this->child_->Next(&child_output);
	if (child_output.empty() || child_output.at(0).content_ == nullptr) {
	  ((std::vector<toyDBTUPLE> *)dst)->clear();
	  ((std::vector<toyDBTUPLE> *)dst)->resize(BATCH_SIZE);
	  break;
	}
	cur_ancestor = child_output.at(0).ancestor_;
	// Here should be a loop iterate thru all emitted tuples
	for (auto it : child_output) {
	  ++cnt;
	  type_idx = type_schema.typeID2type[it.type_ids_[col_idx]];
	  switch (type_schema.typeID2type[it.type_ids_[col_idx]]) {
		case (1): {
		  auto tmp = (long long *)result_;
		  *tmp += (int)(*(it.content_ + col_offset));
		  std::memcpy(result_, tmp, sizeof(long long));
		  break;
		}
		case (2): {
		  auto tmp = (double *)result_;
		  *tmp += (double)(*(it.content_ + col_offset));
		  std::memcpy(result_, tmp, sizeof(double));
		  break;
		}
		case (3): {
		  auto tmp = (unsigned long long *)result_;
		  *tmp += (unsigned long long)(*(it.content_ + col_offset));
		  std::memcpy(result_, tmp, sizeof(unsigned long long));
		  break;
		}
		default: break;
	  }
	}
  }
  switch (type_idx) {
	case (1): {
	  auto tmp = (long long *)this->result_;
	  *tmp /= cnt;
	  std::memcpy(this->result_, tmp, sizeof(long long));
	  break;
	}
	case (2): {
	  auto tmp = (double *)this->result_;
	  *tmp /= (double)cnt;
	  std::memcpy(this->result_, tmp, sizeof(double));
	  break;
	}
	case (3): {
	  auto tmp = (unsigned long long *)this->result_;
	  *tmp /= cnt;
	  std::memcpy(this->result_, tmp, sizeof(unsigned long long));
	  break;
	}
	default:break;
  }
  toyDBTUPLE tmp((char *)this->result_, 64, {64}, {typeid(size_t).hash_code()});
  tmp.ancestor_ = cur_ancestor;
  tmp.table_ = this->view_->GetID();

  ((std::vector<toyDBTUPLE> *)dst)->at(0) = tmp;
  this->finished_ = true;
}
