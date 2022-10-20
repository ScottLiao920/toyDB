// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/20/2022.
//
#include "executor.h"
#include "schema.h"

void nestedLoopJoinExecutor::Init() {
  executor::Init();
  this->left_child_->Init();
  this->right_child_->Init();
  this->view_->SetName("Nested Loop Join View for Tab {" + this->left_child_->GetViewName() + "} and {"
						   + this->right_child_->GetViewName() + "}");
  this->view_->cols_ = table_schema.TableName2Table[this->left_child_->GetViewName()]->cols_;
  for (auto &it : this->view_->cols_) {
	it.SetName(this->left_child_->GetViewName() + '.' + it.getName());
//	it.SetTable(this->left_child_->GetViewID());
  }
  std::vector<std::string> tmp;
  std::string
	  left_col = std::get<3>(this->pred_->data_srcs[0]).substr(std::get<3>(this->pred_->data_srcs[0]).find(".") + 1);
  std::string
	  right_col = std::get<3>(this->pred_->data_srcs[1]).substr(std::get<3>(this->pred_->data_srcs[0]).find(".") + 1);
  for (auto it : table_schema.TableName2Table[this->right_child_->GetViewName()]->cols_) {
//	std::string upp_name = it.getName();
//	std::transform(upp_name.begin(), upp_name.end(), upp_name.begin(), ::toupper);
//	if (upp_name != left_col && upp_name != right_col) {
//	  std::string tmp_name = this->right_child_->GetViewName() + '.' + it.getName();
	this->view_->cols_.push_back(it);
	this->view_->cols_.back().SetName(this->right_child_->GetViewName() + '.' + it.getName());
//	  it.SetTable(this->right_child_->GetViewID());
//	}
  }
  this->curLeftBatch_ = std::vector<toyDBTUPLE>(BATCH_SIZE);
  this->curRightBatch_ = std::vector<toyDBTUPLE>(BATCH_SIZE);
  this->left_child_->Next(&this->curLeftBatch_);
  this->curLeftTuple_ = this->curLeftBatch_.begin();
  this->right_child_->Next(&this->curRightBatch_);
  this->curRightTuple_ = this->curRightBatch_.begin();
}
void nestedLoopJoinExecutor::Next(void *dst) {
  size_t cnt = 0;
  if (this->curLeftTuple_->content_ == nullptr) {
	// All tuple emitted. Set dst to an empty vector
	((std::vector<toyDBTUPLE> *)dst)->clear();
	return;
  }
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
//	std::cout << "Comparing " << (int)*(left.content_ + l_col_offset) << " from left side with "
//			  << (int)*(right.content_ + r_col_offset) << " from right side" << std::endl;
	if (this->pred_ == nullptr || this->pred_->compareFunc(this->curLeftTuple_->content_ + l_col_offset,
														   this->curRightTuple_->content_ + r_col_offset)) {
	  // pass predicate, join this two tuple tgt
	  toyDBTUPLE *tup = this->Join(this->curLeftTuple_.base(), this->curRightTuple_.base());
	  tup->ancestor_ = this->curLeftTuple_->ancestor_;
	  tup->table_ = this->view_->GetID();
	  ((std::vector<toyDBTUPLE> *)dst)->at(cnt) = *tup;
	  ++cnt;
	}
	// Finished evaluating curRightTuple and curLeftTuple, move to the next pair
	this->curRightTuple_ = std::next(this->curRightTuple_);
	if (this->curRightTuple_ == curRightBatch_.end()) {
	  // Current batch of right tuples all validated, retrieve the next batch
	  this->right_child_->Next(&this->curRightBatch_);
	  if (this->curRightBatch_.empty() || this->curRightBatch_.begin()->content_ == nullptr) {
		// iterate thru all right tuples already, get to the next left tuple
		((seqScanExecutor *)
			this->right_child_)->Reset(); // TODO: this should be compatible any other executors.
		this->curLeftTuple_ = std::next(this->curLeftTuple_);
		if (this->curLeftTuple_ == this->curLeftBatch_.end()) {
		  // Reached end of current block, retrieve the next block
		  this->left_child_->Next(&this->curLeftBatch_);
		  if (this->curLeftBatch_.empty() || this->curLeftBatch_.begin()->content_ == nullptr) {
			// iterate thru all left & right tuples, just return
			return;
		  }
		  this->curLeftTuple_ = this->curLeftBatch_.begin();
		}
		this->curRightBatch_.resize(BATCH_SIZE); // Need to reserve/resize since it might be cleared
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