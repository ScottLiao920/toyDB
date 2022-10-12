// Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
// Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
// Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
// Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
// Vestibulum commodo. Ut rhoncus gravida arcu.

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

TypeSchema type_schema;

#endif //TOYDB_SRC_INCLUDE_TYPE_H_
