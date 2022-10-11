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
#include <numeric>
#include "config.h"

typedef unsigned int HEAP_PAGE_ID;
typedef unsigned int PhysicalPageID;
#define INVALID_PHYSICAL_PAGE_ID 0xFFFFFFFF
typedef unsigned int RelID;
typedef unsigned int RowID;
typedef unsigned int ColID;

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

#endif //TOYDB_COMMON_H
