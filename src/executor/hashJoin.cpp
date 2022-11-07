// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/31/2022.
//

#include "executor.h"
#include "schema.h"
#include "type.h"

void hashJoinExecutor::Init() {
  executor::Init();
  this->left_child_->Init();
  this->right_child_->Init();
  this->view_->SetName("Hash Join View for Tab {" + this->left_child_->GetViewName() + "} and {"
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
	this->view_->cols_.push_back(it);
	this->view_->cols_.back().SetName(this->right_child_->GetViewName() + '.' + it.getName());
  }

  this->col_name = std::get<3>(this->pred_->data_srcs[0]).substr(std::get<3>(this->pred_->data_srcs[0]).find('.') + 1);
  // Also builds a hash table on left child.
  std::vector<toyDBTUPLE> buf;
  while (true) {
	this->left_child_->Next(&buf);
	if (buf.empty() || buf.begin()->content_ == nullptr) {
	  break;
	}
	rel *l_tab = table_schema.TableID2Table[buf.begin()->table_];
	size_t offset = l_tab->GetOffset(col_name);
	size_t view_offset = 0, cnt = 0;
	switch (type_schema.typeID2type[buf.begin()->type_ids_[l_tab->GetColIdx(col_name)]]) {
	  case (INT): {
		if (this->left_index_tree_ == nullptr) {
		  this->left_index_tree_ = new bTree<int>(FAN_OUT_FACT);
		}
		for (const auto &it : buf) {
		  ((bTree<int> *)this->left_index_tree_)->insert(*((int *)it.content_ + offset), tupleLocType(0, cnt));
		  std::memcpy(this->mem_context_ + view_offset, it.content_, it.size_);
		  ++cnt;
		  view_offset += it.size_;
		}
		break;
	  }
	  case (FLOAT): {
		if (this->left_index_tree_ == nullptr) {
		  this->left_index_tree_ = new bTree<float>(FAN_OUT_FACT);
		}
		for (const auto &it : buf) {
		  ((bTree<float> *)this->left_index_tree_)->insert(*((float *)it.content_ + offset), tupleLocType(0, cnt));
		  std::memcpy(this->mem_context_ + view_offset, it.content_, it.size_);
		  ++cnt;
		  view_offset += it.size_;
		}
		break;
	  }
	  case (SIZE_T): {
		if (this->left_index_tree_ == nullptr) {
		  this->left_index_tree_ = new bTree<size_t>(FAN_OUT_FACT);
		}
		for (const auto &it : buf) {
		  ((bTree<size_t> *)this->left_index_tree_)->insert(*((size_t *)it.content_ + offset), tupleLocType(0, cnt));
		  std::memcpy(this->mem_context_ + view_offset, it.content_, it.size_);
		  ++cnt;
		  view_offset += it.size_;
		}
		break;
	  }
	  case (STRING): {
		std::cout << "Trying to build Index on a string!";
	  }
	}
  }
}
void hashJoinExecutor::Next(void *dst) {
  std::vector<toyDBTUPLE> buf;
  while (true) {
	this->right_child_->Next(&buf);
	if (buf.empty() || buf.begin()->content_ == nullptr) {
	  ((std::vector<toyDBTUPLE> *)dst)->clear();
	  break;
	}
	rel *r_tab = table_schema.TableID2Table[buf.begin()->table_];
	for (const auto &it : buf) {
	  bool found = false;
	  size_t offset = r_tab->GetOffset(this->col_name);
	  switch (type_schema.typeID2type[buf.begin()->type_ids_[r_tab->GetColIdx(this->col_name)]]) {
		case (INT): {
		  found = (((bTree<int> *)this->left_index_tree_)->search(*((int *)it.content_ + offset)) == nullptr);
		  break;
		}
		case (FLOAT): {
		  found = (((bTree<float> *)this->left_index_tree_)->search(*((float *)it.content_ + offset)) == nullptr);
		  break;
		}
		case (SIZE_T): {
		  found = (((bTree<size_t> *)this->left_index_tree_)->search(*((size_t *)it.content_ + offset)) == nullptr);
		  break;
		}
		case (STRING): {
		  std::cout << "Trying to search on a string!";
		}
	  }
	  if (found) {

	  }
	}
  }
}
void nestedLoopJoinExecutor::End() {
  executor::End();
  this->left_child_->End();
  this->right_child_->End();
}