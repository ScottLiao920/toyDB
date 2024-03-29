// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/10/2022.
//

#ifndef TOYDB_SRC_INCLUDE_CONFIG_H_
#define TOYDB_SRC_INCLUDE_CONFIG_H_

#define FILEPATH "/mnt/c/users/scorp/ClionProjects/toyDB/data/"
//#define INVALID_PHYSICAL_PAGE_ID 0
#define PHYSICAL_PAGE_SIZE 8192
#define HEAP_SIZE 8192
#define BUFFER_POOL_SIZE 16
#define EXEC_MEM (8192*2)
#define BATCH_SIZE 1
#define FAN_OUT_FACT 5

enum storageMethod {
  row_store, col_store
};

enum execution_mode {
  volcano, vector
};

enum index_type {
  btree, hash
};

enum join_type {
  nestedLoopJoin, mergeJoin, hashJoin
};

enum scan_type {
  sequentialScan, indexScan, bitMapIndexScan
};

#endif //TOYDB_SRC_INCLUDE_CONFIG_H_
