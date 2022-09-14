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

class heapPage {
public:
    char content[HEAP_SIZE];
    time_t timestamp;

    heapPage();

    ~heapPage();
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

    void readFromDisk(PhysicalPageID);

    void writeToDisk(PhysicalPageID, heapPage);

    void printContent(int idx) { std::cout << idx << this->pages_[idx].content << std::endl; }

    heapPage *evict();

};


#endif //TOYDB_BUFFERPOOL_H
