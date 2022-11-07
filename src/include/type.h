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
	typeID2type[typeid(int).hash_code()] = SUPPORTED_TYPES::INT;
	typeID2type[typeid(float).hash_code()] = SUPPORTED_TYPES::FLOAT;
	typeID2type[typeid(size_t).hash_code()] = SUPPORTED_TYPES::SIZE_T;
	typeID2type[typeid(std::string).hash_code()] = SUPPORTED_TYPES::STRING;
  }
};

static TypeSchema type_schema;

#endif //TOYDB_SRC_INCLUDE_TYPE_H_
