// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by scorp on 10/12/2022.
//

#ifndef TOYDB_SRC_INCLUDE_TYPE_H_
#define TOYDB_SRC_INCLUDE_TYPE_H_
#include <map>

class TypeSchema {
 public:
  std::map<size_t, size_t> typeID2type;
  TypeSchema() {
	typeID2type[typeid(int).hash_code()] = 1;
	typeID2type[typeid(float).hash_code()] = 2;
	typeID2type[typeid(size_t).hash_code()] = 3;
	typeID2type[typeid(std::string).hash_code()] = 4;
  }
};

static TypeSchema type_schema;

#endif //TOYDB_SRC_INCLUDE_TYPE_H_
