//
// Created by liaoc on 9/13/22.
//

#include <cstring>
#include <utility>
#include <ctime>
#include "storage.h"

physicalPage::physicalPage(PhysicalPageID id) {
  std::ofstream cur_page_file;
  this->cur_id_ = id;
  std::string file_name = FILEPATH;
  file_name.append(std::to_string(this->cur_id_));
  this->file_path_ = file_name;
  cur_page_file.open(file_name, std::ios::out);
  cur_page_file.close();
  this->cur_id_ += 1;
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

column::column(std::string inp_name, size_t size, RelID par_table) {
  this->name_ = inp_name;
  this->width_ = size;
  this->rel_ = par_table;
  this->cnt_ = 0;
}

void rel::set_name_(std::string inp) {
  this->name_ = std::move(inp);
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

void rel::add_columns(const std::vector<std::string> &names, const std::vector<size_t> &widths) {
  for (std::vector<size_t>::size_type i = 0; i < names.size(); ++i) {
	this->cols_.emplace_back(names[i], widths[i], this->relId_);
  }
}

void rel::add_row(const size_t &inp_size) {
  this->rows_.emplace_back(inp_size, this->relId_);
}

void rel::add_row(const row &inp_row) {
  this->rows_.push_back(inp_row);
}

void rel::add_column(const std::string &inp_name, const size_t &inp_size) {
  this->cols_.emplace_back(inp_name, inp_size, this->relId_);
}

void rel::add_column(const column &inp_column) {
  this->cols_.push_back(inp_column);
}

bool rel::set_scheme_(storageMethod method) {
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

row::row(size_t size, RelID par_table) {
  this->id_ = std::time(nullptr);
  this->width_ = size;
  this->par_ = par_table;
}

void row::insert(bufferPoolManager *bpmgr, char *content) {
  if (this->pages_.empty()) {
	this->pages_.push_back(bpmgr->stmgr_->addPage() - 1);
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
