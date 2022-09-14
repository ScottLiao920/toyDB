//
// Created by liaoc on 9/14/22.
//

#include <cstring>
#include <chrono>
#include "bufferpool.h"


bufferPoolManager::bufferPoolManager(storageManager *stmgr) {
    this->stmgr_ = stmgr;
    // TODO: initialize the pages here
    this->pages_.reserve(this->no_pages_);
    this->page_table_.reserve(this->no_pages_);
    this->is_dirty_.reserve(this->no_pages_);
    this->pin_cnt_.reserve(this->no_pages_);
}

heapPage *bufferPoolManager::evict() {
    // LRU replacement
    std::time_t oldest = this->pages_.cbegin()->timestamp;
    int cnt, oldest_idx = 0;
    for (cnt = 0; cnt < this->no_pages_; ++cnt) {
        if (this->pin_cnt_[cnt] == 0) {
            if (this->pages_[cnt].timestamp < oldest) {
                if (this->is_dirty_[cnt]) {
                    // write dirty page to disk before evict it;
                    this->writeToDisk(this->page_table_[&this->pages_[cnt]], this->pages_[cnt]);
                }
                std::memset(this->pages_[cnt].content, 0, HEAP_SIZE);
            }
        }
    }
    return &this->pages_[cnt];
}

void bufferPoolManager::readFromDisk(PhysicalPageID psy_id) {
    int cnt = 0;
    for (auto &it: this->page_table_) {
        if (it.second == INVALID_PHYSICAL_PAGE_ID) {
            it.second = psy_id;
            this->stmgr_->readPage(psy_id, it.first->content);
            it.first->timestamp = std::time(nullptr);
            this->pin_cnt_[cnt] += 1;
            this->is_dirty_[cnt] = false;
            return;
        }
    }
    heapPage *cleanedPage = evict();
    this->stmgr_->readPage(psy_id, cleanedPage->content);
    cleanedPage->timestamp = std::time(nullptr);
}

void bufferPoolManager::writeToDisk(PhysicalPageID psy_id, heapPage page) {
    this->stmgr_->writePage(psy_id, page.content);
}

heapPage::heapPage() {
    this->timestamp = std::time(nullptr);
    this->content = (char *) std::malloc((std::size_t) HEAP_SIZE);
}
