//
// Created by liaoc on 9/14/22.
//

#ifndef TOYDB_BUFFERPOOL_H
#define TOYDB_BUFFERPOOL_H

#include "common.h"
#include "storage.h"

class storageManager;

class heapPage {
 public:
  char content[HEAP_SIZE]{};
  HEAP_PAGE_ID id;
  time_t timestamp;
  size_t data_cnt;
  char *idx_ptr;
  char *data_ptr;

  heapPage();

  ~heapPage();

  bool insert(const char *, size_t);

//    void remove(int);
};

class bufferPoolManager {
 private:
  std::unordered_map<heapPage *, PhysicalPageID> page_table_;
  std::vector<heapPage> pages_;
  std::vector<short> pin_cnt_;
  std::vector<bool> is_dirty_;
  size_t no_pages_ = BUFFER_POOL_SIZE;

 public:
  explicit bufferPoolManager(storageManager *);

  heapPage *findPage(PhysicalPageID);

  void insertToFrame(int, const char *, size_t); // insert content of size size_t pointed by const char * to frame int

  void insertToFrame(heapPage *, const char *, size_t);

  void readFromDisk(PhysicalPageID);

  void writeToDisk(PhysicalPageID, size_t);

  void writeToDisk(PhysicalPageID, heapPage *);

  void printContent(int idx);

  heapPage *evict();

  ~bufferPoolManager();

  storageManager *stmgr_;
};

#endif //TOYDB_BUFFERPOOL_H
