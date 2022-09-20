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
    PhysicalPageID cur_id_{};
    std::string file_path_;

public:
    physicalPage() = default;

    explicit physicalPage(PhysicalPageID);

    void readPage(char *);

    void writePage(const char *);
};

class storageManager {
private:
    PhysicalPageID cur_page_id_ = 0;
    std::vector<physicalPage> pages_;
public:
    storageManager() = default;;

    PhysicalPageID addPage();

    void writePage(PhysicalPageID, void *);

    void readPage(PhysicalPageID, void *);
};

class column {
    std::string name;
    size_t width;
    size_t cnt;
    RelID rel;
    std::vector<PhysicalPageID> pages;
public:
    column(std::string, size_t, RelID);
//    std::vector<>
};

class row {
    std::string name;
    size_t width;
    size_t cnt;
    RelID rel;
    std::vector<PhysicalPageID> pages;
public:
    row(std::string, size_t, RelID);

    void insert(storageManager, char *);
};

class rel {
    RelID id;
    std::string name;
    std::vector<column> cols;
    std::vector<row> rows;
    storageMethod storage_method = row_store;

public:
    rel() = default;

    bool set_scheme_(storageMethod);

    void set_name_(std::string);

    void add_rows(std::vector<row>);

    void add_rows(const std::vector<std::string> &, const std::vector<size_t> &);

    void add_row(const row &);

    void add_row(const std::string &, const size_t &);

    void update_row(bufferPoolManager, std::vector<row>::size_type, char *);

    void add_columns(std::vector<column>);

    void add_columns(const std::vector<std::string> &, const std::vector<size_t> &);

    void add_column(const column &);

    void add_column(const std::string &, const size_t &);

};

#endif //TOYDB_STORAGE_H
