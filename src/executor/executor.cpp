// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 9/22/2022.
//
#include "executor.h"

#include <iomanip>
#include "btree.h"
#include "common.h"
#include "schema.h"
#include "type.h"

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
//  executor::Init(); // no need to initialize views.
  auto table = new rel;
  table->SetName(this->name_);
  for (const auto &it : this->cols_) {
	table->add_column(std::get<3>(it), std::get<1>(it), std::get<2>(it));
  }
}
void indexExecutor::Init() {
  executor::Init();
}
void indexExecutor::Init(bufferPoolManager *bpmgr, rel *tab, size_t idx, index_type = btree) {
  executor::Init();
  this->view_->SetName("Index Builder View for Tab " + tab->GetName());
  this->view_->cols_ = tab->cols_;
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
toyDBTUPLE *joinExecutor::Join(toyDBTUPLE *left, toyDBTUPLE *right) {
  std::vector<size_t> l_sizes = left->sizes_;
  std::vector<size_t> f_types, f_sizes;
  std::vector<size_t> r_sizes = right->sizes_;
  size_t ttl_size = left->size_ + right->size_;
  char *buf = talloc(ttl_size);
  // Write to result tuple in the same order as the column order in view.

//  std::memcpy(buf, left->content_, left->size_);
  size_t offset_f = 0, offset_r = 0, offset_l = 0;
  size_t l_cnt = 0, r_cnt = 0;
  for (auto it : this->view_->cols_) {
	if (it.GetRelID() == left->ancestor_) { //it: original rel id; left->table: view id of scan executor
	  std::memcpy(buf + offset_f, left->content_ + offset_l, l_sizes[l_cnt]);
	  f_types.push_back(left->type_ids_[l_cnt]);
	  f_sizes.push_back(l_sizes[l_cnt]);
	  offset_f += l_sizes[l_cnt];
	  offset_l += l_sizes[l_cnt];
	  ++l_cnt;
	} else {
	  std::memcpy(buf + offset_f, right->content_ + offset_r, r_sizes[r_cnt]);
	  f_types.push_back(right->type_ids_[r_cnt]);
	  f_sizes.push_back(r_sizes[r_cnt]);
	  offset_f += r_sizes[r_cnt];
	  offset_r += r_sizes[r_cnt];
	  ++r_cnt;
	}
  }
  auto *out = (toyDBTUPLE *)talloc(sizeof(toyDBTUPLE));
  out->content_ = buf;
  out->sizes_ = f_sizes;
  out->size_ = ttl_size;
  out->type_ids_ = f_types;
  return out;
}
void updateExecutor::Init() {
//  executor::Init();
  this->table_->add_row(0);
  this->table_->update_row(this->bpmgr_, this->cnt_, this->mem_context_);

}

void copyExecutor::Init() {
//  executor::Init(); // no need to initialize views.
  rel *tab = table_schema.TableName2Table[this->name_];
  if (tab == nullptr) {
	std::cout << "Table " << this->name_ << " not found!" << std::endl;
	return;
  }
  if (this->is_from_) {
	std::ifstream fin;
	fin.open(this->file_path);
	std::string cur_line;
	char buf[tab->GetTupleSize()];
	std::string cur_entry;
	size_t offset, cnt = 0;
	while (true) {
	  std::getline(fin, cur_line);
	  if (cur_line.empty())
		break;
	  offset = 0;
	  std::memset(buf, 0, tab->GetTupleSize());
	  for (auto &it : tab->cols_) {
		cur_entry = cur_line.substr(0, cur_line.find(','));
		cur_line = cur_line.substr(cur_line.find(',') + 1);
		switch (type_schema.typeID2type[it.typeid_]) {
		  case (SUPPORTED_TYPES::INT): {
			int tmp = std::stoi(cur_entry);
			std::memcpy(buf + offset, &tmp, sizeof(int));
			offset += sizeof(int);
			break;
		  }
		  case (SUPPORTED_TYPES::FLOAT): {
			float tmp = std::stof(cur_entry);
			std::memcpy(buf + offset, &tmp, sizeof(float));
			offset += sizeof(float);
			break;
		  }
		  case (SUPPORTED_TYPES::SIZE_T): {
			size_t tmp = std::stoul(cur_entry);
			std::memcpy(buf + offset, &tmp, sizeof(size_t));
			offset += sizeof(size_t);
			break;
		  }
		  case (SUPPORTED_TYPES::STRING): {
			const char *tmp = cur_entry.c_str();
			std::memcpy(buf + offset, tmp, cur_entry.size());
			offset += it.GetSize();
			break;
		  }
		}
	  }
	  PhysicalPageID page_id;
	  if (!tab->rows_.empty()
		  && bpmgr_->stmgr_->GetPage(tab->rows_.back().pages_.back())->free_space_ >= tab->GetTupleSize()) {
		page_id = tab->rows_.back().pages_.back();
	  } else {
		page_id = bpmgr_->stmgr_->addPage();
	  }
	  tab->add_row(tab->GetTupleSize());
	  tab->rows_.back().pages_.emplace_back(page_id);
	  tab->update_row(this->bpmgr_, cnt, buf);
	  ++cnt;
	}
	fin.close();
  } else {
	std::ofstream fout;
	fout.open(this->file_path);
	auto child = new seqScanExecutor;
	child->SetBufferPoolManager(bpmgr_);
	child->SetTable(tab);
	child->Init();
	std::vector<toyDBTUPLE> out(BATCH_SIZE);
	char buf[tab->GetTupleSize()];
	size_t offset;
	while (true) {
	  child->Next(&out);
	  if (out.empty() || out.begin()->content_ == nullptr) {
		break;
	  }
	  for (auto &it : out) {
		offset = 0;
		for (size_t i = 0; i < it.sizes_.size(); ++i) {
		  switch (type_schema.typeID2type[it.type_ids_[i]]) {
			case (SUPPORTED_TYPES::INT): {
			  std::memcpy(buf, it.content_ + offset, it.sizes_[i]);
			  fout << (int)*buf << ",";
			  offset += sizeof(int);
			  break;
			}
			case (SUPPORTED_TYPES::FLOAT): {
			  std::memcpy(buf, it.content_ + offset, it.sizes_[i]);
			  fout << (float)*buf << ",";
			  offset += sizeof(float);
			  break;
			}
			case (SUPPORTED_TYPES::SIZE_T): {
			  std::memcpy(buf, it.content_ + offset, it.sizes_[i]);
			  fout << (size_t)*buf << ",";
			  offset += sizeof(size_t);
			  break;
			}
			case (SUPPORTED_TYPES::STRING): {
			  std::memcpy(buf, it.content_ + offset, it.sizes_[i]);
			  fout << buf << ",";
			  offset += it.sizes_[i];
			  break;
			}
		  }
		}
		fout << std::endl;
	  }
	}
	fout.close();
  }
}
