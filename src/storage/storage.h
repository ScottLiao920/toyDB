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

class storageManager : physicalPage {
private:
    PhysicalPageID cur_page_id_ = 0;
    std::vector<physicalPage> pages_;
public:
    storageManager() = default;;

    PhysicalPageID addPage();

    void writePage(PhysicalPageID, void *);

    void readPage(PhysicalPageID, void *);
};


#endif //TOYDB_STORAGE_H
