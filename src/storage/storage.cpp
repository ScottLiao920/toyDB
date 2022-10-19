// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/13/22.
//

#include <cstring>
#include <utility>
#include <ctime>
#include <algorithm>
#include "storage.h"
#include "bufferpool.h"
#include "schema.h"

TableSchema table_schema;
physicalPage::physicalPage(PhysicalPageID id) {
  std::ofstream cur_page_file;
  this->cur_id_ = id;
  std::string file_name = FILEPATH;
  file_name.append(std::to_string(this->cur_id_));
  this->file_path_ = file_name;
  cur_page_file.open(file_name, std::ios::out);
  cur_page_file.close();
}

void physicalPage::readPage(char *dst) {
  std::ifstream cur_page_file;
  char *buffer = (char *)malloc(this->page_size_);
  cur_page_file.open(this->file_path_);
  cur_page_file.read(buffer, this->page_size_);
  memcpy(dst, buffer, this->page_size_);
  free(buffer);
}

void physicalPage::writePage(const char *content) {
  std::ofstream cur_page_file;
  cur_page_file.open(this->file_path_);
  cur_page_file.write(content, this->page_size_);
  cur_page_file.close();
}

PhysicalPageID storageManager::addPage() {
  physicalPage page(this->cur_page_id_);
  this->pages_.push_back(page);
  return this->cur_page_id_++; // increment the page id, but return the page just added
}

void storageManager::writePage(PhysicalPageID page_id, void *content) {
  if (page_id >= this->cur_page_id_) {
	addPage();
	this->writePage(page_id, content);
	return;
  }
  pages_[page_id].writePage((const char *)content);
}

void storageManager::readPage(PhysicalPageID page_id, void *dst) {
  pages_[page_id].readPage((char *)dst);
}

column::column(std::string inp_name, size_t size, RelID par_table, const std::type_info &type) {
  this->name_ = inp_name;
  this->width_ = size;
  this->typeid_ = type.hash_code();
  this->rel_ = par_table;
  this->cnt_ = 0;
}

void rel::SetName(std::string inp) {
  this->name_ = std::move(inp);
  std::transform(this->name_.begin(), this->name_.end(), this->name_.begin(), ::toupper);
  table_schema.TableName2Table[this->name_] = this;
  table_schema.Table2IDName[this] = std::tie(this->relId_, this->name_);
}

void rel::add_rows(std::vector<row> inp_rows) {
  this->rows_ = inp_rows;
}

void rel::add_columns(std::vector<column> inp_cols) {
  this->cols_ = inp_cols;
}

void rel::add_rows(const std::vector<size_t> &widths) {
  for (std::vector<size_t>::size_type i = 0; i < widths.size(); ++i) {
	this->rows_.emplace_back(widths[i], this->relId_);
  }
}

void rel::add_columns(const std::vector<std::string> &names,
					  const std::vector<size_t> &widths,
					  const std::vector<std::type_info> &types) {
  for (std::vector<size_t>::size_type i = 0; i < names.size(); ++i) {
	this->cols_.emplace_back(names[i], widths[i], this->relId_, types[i]);
  }
}

void rel::add_row(const size_t &inp_size) {
  this->rows_.emplace_back(inp_size, this->relId_);
}

void rel::add_row(const row &inp_row) {
  this->rows_.push_back(inp_row);
}

void rel::add_column(const std::string &inp_name, const size_t &inp_size, const std::type_info &type_info) {
  this->cols_.emplace_back(inp_name, inp_size, this->relId_, type_info);
}

void rel::add_column(const column &inp_column) {
  this->cols_.push_back(inp_column);
}

bool rel::SetScheme(storageMethod method) {
  if (this->cols_.empty() and this->rows_.empty()) {
	this->storage_method_ = method;
	return true;
  } else { return false; }
}

void rel::update_row(bufferPoolManager *bpmgr, std::vector<row>::size_type idx, char *content) {
  if (this->storage_method_ == row_store) {
	if (this->rows_.empty()) {
	  std::cout << "No row found in " << this->name_ << std::endl;
	} else {
	  this->rows_[idx].insert(bpmgr, content);
	}
  } else {
	// What to expect to insert a row in column-store?
	return;
  }
}
std::vector<PhysicalPageID> rel::get_location() {
  std::vector<PhysicalPageID> result;
  if (this->storage_method_ == row_store) {
	for (const auto &it : this->rows_) {
	  result.insert(result.end(), it.pages_.begin(), it.pages_.end());
	}
  } else {
	for (const auto &it : this->cols_) {
	  result.insert(result.end(), it.pages_.begin(), it.pages_.end());
	}
  }
  return result;
}
size_t rel::get_tuple_size() {
  if (this->storage_method_ == row_store) {
	return this->rows_[0].getSize();
  } else {
	size_t out = 0;
	for (auto &it : this->cols_) {
	  out += it.getSize();
	}
	return out;
  }
}
std::vector<size_t> rel::GetColSizes() {
  std::vector<size_t> out;
  for (auto &it : this->cols_) {
	out.push_back(it.getSize());
  }
  return out;
}
rel::rel() {
  this->relId_ = std::time(nullptr);
  while (table_schema.TableID2Table.find(this->relId_) != table_schema.TableID2Table.end()) {
	this->relId_ = std::time(nullptr);
  }
  table_schema.TableID2Table[this->relId_] = this;
  table_schema.Table2IDName[this] = std::tie(this->relId_, "No Name");
}
rel::rel(const std::string &name) {
  this->relId_ = std::time(nullptr);
  while (table_schema.TableID2Table.find(this->relId_) != table_schema.TableID2Table.end()) {
	this->relId_ = std::time(nullptr);
  }
  table_schema.TableID2Table[this->relId_] = this;
  table_schema.Table2IDName[this] = std::tie(this->relId_, "No Name");
  this->SetName(name);
}
size_t rel::GetOffset(const std::string &col_name) {
  std::vector<size_t> sizes = this->GetColSizes();
  size_t col_offset = 0, cnt = 0;
  for (auto it : this->cols_) {
	std::string upper_name = it.getName();
	std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
	if (col_name == upper_name) {
	  break;
	}
	col_offset += sizes[cnt];
	++cnt;
  }
  return col_offset;
}
size_t rel::GetColIdx(const std::string &col_name) {
  size_t col_idx = 0;
  for (auto it : this->cols_) {
	std::string upper_name = it.getName();
	std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);
	if (col_name == upper_name) {
	  break;
	}
	col_idx += 1;
  }
  return col_idx;
}
std::vector<size_t> rel::GetTypeIDs() {
  std::vector<size_t> out(this->cols_.size());
  size_t cnt = 0;
  for (const auto &it : this->cols_) {
	out.at(cnt) = it.typeid_;
	++cnt;
  }
  return out;
}

row::row(size_t size, RelID par_table) {
  this->id_ = std::time(nullptr);
  this->width_ = size;
  this->par_ = par_table;
}

void row::insert(bufferPoolManager *bpmgr, char *content) {
  if (this->pages_.empty()) {
	this->pages_.push_back(bpmgr->stmgr_->addPage());
  }
  heapPage *cur_frame = bpmgr->findPage(this->pages_.back());
  if (cur_frame == nullptr) {
	// the last page of this row is not in memory, read it first
	bpmgr->readFromDisk(this->pages_.back());
	cur_frame = bpmgr->findPage(this->pages_.back());
  }
  bpmgr->insertToFrame(cur_frame, content, this->width_);
  bpmgr->writeToDisk(this->pages_.back(), cur_frame);
}
toyDBTUPLE::toyDBTUPLE(char *buf, size_t len, std::vector<size_t> sizes, std::vector<size_t> type_ids) {
  this->content_ = (char *)new char[len];
  std::memcpy(this->content_, buf, len);
  this->size_ = len;
  this->sizes_ = std::vector<size_t>(std::move(sizes));
  this->type_ids_ = std::vector<size_t>(std::move(type_ids));
  if (std::accumulate(this->sizes_.cbegin(), this->sizes_.cend(), (size_t)0) != this->size_) {
	std::cout << "Check size!" << std::endl;
  }
}
//toyDBTUPLE::toyDBTUPLE(const toyDBTUPLE &ref) {
//  this->size_ = ref.size_;
//  content_ = (char *)new char[size_];
//  std::memcpy(this->content_, ref.content_, this->size_);
//  this->table_ = ref.table_;
//  this->row_ = ref.row_;
//  this->sizes_ = std::vector<size_t>(ref.sizes_);
//}
toyDBTUPLE::~toyDBTUPLE() {
//  delete[] this->content_;
}

toyDBTUPLE &toyDBTUPLE::operator=(const toyDBTUPLE &ref) {
  if (&ref != this) {
	this->size_ = ref.size_;
	if (ref.content_ == nullptr) {
	  this->content_ = nullptr;
	} else {
	  this->content_ = (char *)new char[size_];
	  std::memcpy(this->content_, ref.content_, this->size_);
	}
	this->table_ = ref.table_;
	this->row_ = ref.row_;
	this->sizes_ = std::vector<size_t>();
	for (auto it : ref.sizes_) {
	  this->sizes_.push_back(it);
	}
	this->type_ids_ = std::vector<size_t>();
	for (auto it : ref.type_ids_) {
	  this->type_ids_.push_back(it);
	}
//	this->sizes_ = std::vector<size_t>(ref.sizes_);
  }
  return *this;
}

