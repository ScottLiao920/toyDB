//
// Created by liaoc on 9/13/22.
//

#ifndef TOYDB_STORAGE_H
#define TOYDB_STORAGE_H

#include "common.h"
#include "bufferpool.h"

class bufferPoolManager;

class physicalPage {
 private:
  size_t page_size_ = PHYSICAL_PAGE_SIZE;
  PhysicalPageID cur_id_ = 0;
  std::string file_path_;
  size_t data_cnt_ = 0;
  size_t free_space_ = PHYSICAL_PAGE_SIZE;

 public:
  physicalPage() = default;

  explicit physicalPage(PhysicalPageID);

  void readPage(char *);

  void writePage(const char *);

  size_t getDataCnt() { return this->data_cnt_; }

  size_t getFreeSpace() { return this->free_space_; }
};

class storageManager {
 private:
  PhysicalPageID cur_page_id_ = 0;
  std::vector<physicalPage> pages_;
 public:
  storageManager() = default;;

  size_t getDataCnt(size_t idx) { return this->pages_[idx].getDataCnt(); };

  PhysicalPageID addPage();

  void writePage(PhysicalPageID, void *);

  void readPage(PhysicalPageID, void *);
};

class toyDBTUPLE {
 public:
  char *content_;
  size_t size_;
  RelID table_;
  RowID row_;
  toyDBTUPLE() {
	table_ = INVALID_PHYSICAL_PAGE_ID;
	row_ = INVALID_PHYSICAL_PAGE_ID;
	content_ = nullptr;
  }
  toyDBTUPLE(const toyDBTUPLE &ref) {
	size_ = ref.size_;
	content_ = (char *)std::malloc(size_);
	std::memcpy(content_, ref.content_, size_);
	table_ = ref.table_;
	row_ = ref.row_;
  }
  toyDBTUPLE(char *buf, size_t len) {
	if (this->content_ == nullptr) {
	  this->content_ = (char *)std::malloc(len);
	}
	std::memcpy(this->content_, buf, len);
	this->size_ = len;
  };
};

class column {
  std::string name_;
  size_t width_;
  size_t cnt_;
  RelID rel_;
 public:
  size_t getSize() { return this->width_; }
  column(std::string, size_t, RelID, const std::type_info &);
  std::vector<PhysicalPageID> pages_;
  size_t typeid_;
};

class row {
  RowID id_;
  size_t width_;
  size_t cnt_;
  RelID par_;
 public:
  row(size_t, RelID);
  size_t getSize() { return this->width_; }
  void insert(bufferPoolManager *, char *);
  std::vector<PhysicalPageID> pages_;
  void setPages(std::vector<PhysicalPageID> p) { this->pages_ = p; };
};

class rel {
  // CAUTION: rethink about cols_ and rows_. Does one really need a vector of rows in a row store?
  RelID relId_;
  std::string name_;
  storageMethod storage_method_ = row_store;
 public:
  std::vector<column> cols_;
  std::vector<row> rows_;

  rel() = default;
//  rel(std::vector<std::toyDBTUPLE(size_t, std::string)>);
  storageMethod getStorageMethod() { return this->storage_method_; };
  bool set_scheme_(storageMethod);
  void set_name_(std::string);
  void add_rows(std::vector<row>);
  void add_rows(const std::vector<size_t> &);
  void add_row(const row &);
  void add_row(const size_t &);
  void update_row(bufferPoolManager *, std::vector<row>::size_type, char *);
  void add_columns(std::vector<column>);
  void add_columns(const std::vector<std::string> &, const std::vector<size_t> &, const std::vector<std::type_info> &);
  void add_column(const column &);
  void add_column(const std::string &, const size_t &, const std::type_info &);
  std::vector<PhysicalPageID> get_location();
  size_t get_tuple_size();
};

#endif //TOYDB_STORAGE_H
