//
// Created by liaoc on 9/14/22.
//

#ifndef TOYDB_BUFFERPOOL_H
#define TOYDB_BUFFERPOOL_H

#include <unordered_set>
#include <unordered_map>
#include "../storage/storage.h"

#define HEAP_SIZE 8192
#define BUFFER_POOL_SIZE 1
typedef unsigned int HEAP_PAGE_ID;

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

    void remove(int);
};

class bufferPoolManager {
private:
    std::unordered_map<heapPage *, PhysicalPageID> page_table_;
    std::vector<heapPage> pages_;
    std::vector<short> pin_cnt_;
    std::vector<bool> is_dirty_;
    storageManager *stmgr_;
    size_t no_pages_ = BUFFER_POOL_SIZE;

public:
    explicit bufferPoolManager(storageManager *);

    void insertToFrame(int, const char *, size_t);

    void insertToFrame(heapPage *, const char *, size_t);

    void readFromDisk(PhysicalPageID);

    void writeToDisk(PhysicalPageID, int);

    void printContent(int idx);

    heapPage *evict();

    ~bufferPoolManager();
};


#endif //TOYDB_BUFFERPOOL_H
