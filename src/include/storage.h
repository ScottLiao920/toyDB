// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/13/22.
//

#ifndef TOYDB_STORAGE_H
#define TOYDB_STORAGE_H

#include "common.h"
//#include "bufferpool.h"

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
  // This class serves as an in-memory structure for a tuple;
 public:
  char *content_; // Actual content
  size_t size_; // total length
  RelID table_; // table source
  RelID ancestor_; // The root of toyDBTuple
  RowID row_; // row number in the source table
  std::vector<size_t> sizes_; // sizes of individual columns, the sum of this vector should equal to size_
  std::vector<size_t> type_ids_; // typeid of col types
  toyDBTUPLE() {
	this->table_ = INVALID_PHYSICAL_PAGE_ID;
	this->row_ = INVALID_PHYSICAL_PAGE_ID;
	this->content_ = nullptr;
	this->size_ = 0;
	this->sizes_ = std::vector<size_t>();
	this->type_ids_ = std::vector<size_t>();
  }
//  toyDBTUPLE(const toyDBTUPLE &ref);
  toyDBTUPLE(char *buf, size_t len, std::vector<size_t> sizes, std::vector<size_t> type_ids);
  toyDBTUPLE &operator=(const toyDBTUPLE &);
  ~toyDBTUPLE();
};

class column {
  std::string name_;
  size_t width_;
  size_t cnt_;
  RelID rel_;
 public:
  void SetName(std::string inp) { this->name_ = std::move(inp); }
  size_t getSize() { return this->width_; }
  std::string getName() { return this->name_; }
  column(std::string, size_t, RelID, const std::type_info &);
  void SetTable(RelID id) { this->rel_ = id; }
  std::vector<PhysicalPageID> pages_;
  RelID GetRelID() { return this->rel_; };
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
  /*
   * This class serve as a class for both in-memory view and on-disk tables.
   */
  // CAUTION: rethink about cols_ and rows_. Does one really need a vector of rows in a row store?
  RelID relId_;
  std::string name_;
  storageMethod storage_method_ = row_store;
 public:
  std::vector<column> cols_;
  std::vector<row> rows_;

  rel();
  explicit rel(const std::string &);
//  rel(std::vector<std::toyDBTUPLE(size_t, std::string)>);
  storageMethod getStorageMethod() { return this->storage_method_; };
  bool SetScheme(storageMethod);
  void SetName(std::string);
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
  std::vector<size_t> GetColSizes();
  std::string GetName() { return this->name_; }
  size_t GetOffset(const std::string &);
  size_t GetColIdx(const std::string &);
  std::vector<size_t> GetTypeIDs();
  RelID GetID() { return this->relId_; };
};

#endif //TOYDB_STORAGE_H
