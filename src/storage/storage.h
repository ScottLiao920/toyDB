//
// Created by liaoc on 9/13/22.
//

#ifndef TOYDB_STORAGE_H
#define TOYDB_STORAGE_H

#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>

#define FILEPATH "/home/liaoc/Projects/toyDB/data/"

#define INVALID_PHYSICAL_PAGE_ID 0
typedef unsigned int PhysicalPageID;
typedef unsigned int RelID;
typedef unsigned int RowID;
typedef unsigned int ColID;

enum storageMethod {
    row_store, col_store
};


class physicalPage {
private:
    size_t page_size_ = 8192;
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
    char *name;
    size_t width;
    size_t cnt;
    RelID rel;
};

class rel {
    RelID id;
    std::vector<column> cols;
    std::vector<row> rows;
    storageMethod storage_method;

};

#endif //TOYDB_STORAGE_H
