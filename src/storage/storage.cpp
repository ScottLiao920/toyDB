//
// Created by liaoc on 9/13/22.
//

#include <cstring>
#include <utility>
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

column::column(std::string inp_name, size_t size, RelID par_table) {
    this->name = inp_name;
    this->width = size;
    this->rel = par_table;
}

void rel::set_name_(std::string inp) {
    this->name = std::move(inp);
}

void rel::add_rows(std::vector<row> inp_rows) {
    this->rows = inp_rows;
}

void rel::add_columns(std::vector<column> inp_cols) {
    this->cols = inp_cols;
}

void rel::add_rows(const std::vector<std::string> &names, const std::vector<size_t> &widths) {
    for (std::vector<size_t>::size_type i = 0; i < names.size(); ++i) {
        this->rows.emplace_back(names[i], widths[i], this->id);
    }
}

void rel::add_columns(const std::vector<std::string> &names, const std::vector<size_t> &widths) {
    for (std::vector<size_t>::size_type i = 0; i < names.size(); ++i) {
        this->cols.emplace_back(names[i], widths[i], this->id);
    }
}

void rel::add_row(const std::string &inp_name, const size_t &inp_size) {
    this->rows.emplace_back(inp_name, inp_size, this->id);
}

void rel::add_row(const row &inp_row) {
    this->rows.push_back(inp_row);
}

void rel::add_column(const std::string &inp_name, const size_t &inp_size) {
    this->cols.emplace_back(inp_name, inp_size, this->id);
}

void rel::add_column(const column &inp_column) {
    this->cols.push_back(inp_column);
}


row::row(std::string inp_name, size_t size, RelID par_table) {
    this->name = inp_name;
    this->width = size;
    this->rel = par_table;
}
