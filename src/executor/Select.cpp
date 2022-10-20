// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/20/2022.
//
#include <iomanip>
#include "executor.h"
#include "schema.h"
#include "type.h"

void selectExecutor::Init() {
  executor::Init();
  for (auto child : this->children_) { child->Init(); }
  this->view_->SetName("Selection View" + this->children_[0]->GetViewName());
  for (auto it : this->targetList_) {
	this->view_->cols_.push_back(table_schema.TableID2Table[std::get<0>(it->data_srcs[0])]->cols_[std::get<1>(it->data_srcs[0])]);
  }

}
void selectExecutor::Init(std::vector<expr *> exprs, std::vector<executor *> children) {
  executor::Init();
  for (auto child : this->children_) {
	child->Init();
  }
  this->view_->SetName("Selection View" + children[0]->GetViewName());
  for (auto it : exprs) {
	this->view_->cols_.push_back(table_schema.TableID2Table[std::get<0>(it->data_srcs[0])]->cols_[std::get<1>(it->data_srcs[0])]);
  }
  this->targetList_ = exprs;
  this->children_ = std::move(children);
//  for (auto &it : this->view_->cols_) {
//	it.SetTable(this->children_[0]->GetViewID());
//  }
}
void selectExecutor::Next(void *dst) {
  if (this->cnt_ == 0) {
	std::cout << std::setw(8) << " ";
	for (auto it : this->targetList_) {
	  std::cout << "|" << it->alias;
	}
	std::cout << std::endl;
  }
  for (auto it : children_) {
	auto *buf = new std::vector<toyDBTUPLE>(BATCH_SIZE);
	it->Next(buf);
	if (buf->empty() || buf->at(0).content_ == nullptr) {
	  std::memset(dst, 0, 8);
	  return;
	}
	std::cout << std::setw(8) << this->cnt_;
	std::memset(dst, 0x11, 8);
	if (this->mode_ == volcano) {
	  size_t cnt = 0;
	  for (auto col_size : this->view_->GetColSizes()) {
		// TODO: validate targetList on tmp_buf here
		if (col_size == 0) {
		  // skip the first empty entry
		  continue;
		}
		char tmp_buf[col_size];
		std::vector<column> cand_cols = table_schema.TableID2Table[this->children_[0]->GetViewID()]->cols_;
		column &target_col = this->view_->cols_[cnt];
		size_t buf_offset = 0;
		for (auto cand_col : cand_cols) {
		  std::string cand_name = cand_col.getName();
		  if (cand_col.GetRelID() == target_col.GetRelID()
			  && cand_name.find(target_col.getName()) != std::string::npos) {
			std::memcpy(tmp_buf, buf->cbegin()->content_ + buf_offset, target_col.getSize());
			break;
		  }
		  buf_offset += cand_col.getSize();
		}

//		std::memcpy(tmp_buf, buf->cbegin()->content_ + offset, col_size);
		std::cout << "|" << std::setw(this->targetList_[cnt]->alias.size());
		switch (type_schema.typeID2type[target_col.typeid_]) {
		  case (1): {
			std::cout << (int)*tmp_buf;
			break;
		  }
		  case (2): {
			std::cout << (float)*tmp_buf;
			break;
		  }
		  case (3): {
			std::cout << (size_t)*tmp_buf;
			break;
		  }
		  case (4): {
			std::cout << std::string(tmp_buf);
			break;
		  }
		}
		++cnt;
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
  for (auto it : this->children_) {
	it->End();
  }
  std::cout << "Output " << this->cnt_ << " tuples." << std::endl;
}