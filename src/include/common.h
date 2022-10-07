//
// Created by liaoc on 9/20/22.
//

#ifndef TOYDB_COMMON_H
#define TOYDB_COMMON_H

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <cstring>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <typeindex>
#include <vector>

#define FILEPATH "/mnt/c/users/scorp/ClionProjects/toyDB/data/"
//#define INVALID_PHYSICAL_PAGE_ID 0
#define PHYSICAL_PAGE_SIZE 8192
#define HEAP_SIZE 8192
#define BUFFER_POOL_SIZE 1
#define EXEC_MEM 8192
#define BATCH_SIZE 64

typedef unsigned int HEAP_PAGE_ID;
typedef unsigned int PhysicalPageID;
#define INVALID_PHYSICAL_PAGE_ID 0xFFFFFFFF
typedef unsigned int RelID;
typedef unsigned int RowID;
typedef unsigned int ColID;

enum storageMethod {
  row_store, col_store
};

enum command_type {
  SELECT,
  INSERT,
  UPDATE,
  DELETE,
  INVALID_COMMAND
};

enum comparision {
  equal,
  lt,
  ne,
  gt,
  ngt,
  nlt,
//    like,
  NO_COMP
};

enum aggr {
  MIN, MAX, COUNT, AVG, SUM, NO_AGGR
};

enum expr_type {
  AGGR, COMP, COL
};

enum execution_mode {
  volcano, vector
};

enum index_type {
  btree, hash
};

#endif //TOYDB_COMMON_H
