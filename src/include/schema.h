// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/12/2022.
//

#ifndef TOYDB_SRC_INCLUDE_SCHEMA_H_
#define TOYDB_SRC_INCLUDE_SCHEMA_H_

#include <map>
#include "storage.h"
class TableSchema {
 private:
  std::map<size_t, rel *> TableID2Table;
  std::map<std::string, rel *> TableName2Table;
  friend class rel;
};

TableSchema table_schema;
#endif //TOYDB_SRC_INCLUDE_SCHEMA_H_
