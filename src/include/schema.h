// Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
// Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
// Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
// Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
// Vestibulum commodo. Ut rhoncus gravida arcu.

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
  friend class rel;
};

TableSchema table_schema;
#endif //TOYDB_SRC_INCLUDE_SCHEMA_H_
