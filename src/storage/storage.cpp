//
// Created by liaoc on 9/13/22.
//

#include <cstring>
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
    char *buffer = (char *) malloc(this->page_size_);
    cur_page_file.open(this->file_path_);
    cur_page_file.read(buffer, this->page_size_);
    memcpy(dst, buffer, this->page_size_);
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
    this->cur_page_id_ += 1;
    return this->cur_page_id_;
}

void storageManager::writePage(PhysicalPageID page_id, void *content) {
    if (page_id >= this->cur_page_id_) {
        addPage();
        this->writePage(page_id, content);
        return;
    }
    pages_[page_id].writePage((const char *) content);
}

void storageManager::readPage(PhysicalPageID page_id, void *dst) {
    pages_[page_id].readPage((char *) dst);
}

column::column(std::string name, size_t size, RelID par_table) {
    this->name = name;
    this->width = size;
    this->rel = par_table;
}
