//
// Created by liaoc on 9/15/22.
//

#ifndef TOYDB_PARSER_H
#define TOYDB_PARSER_H

#include "../include/storage.h"
#include "common.h"

class expr {
 public:
  expr_type type;
  std::string alias;
  std::vector<std::string> data_srcs;

  expr();

  ~expr();
};

class aggr_expr : expr {
 public:
  aggr aggr_type;
};

class comparison_expr : expr {
 public:
  comparision comparision_type;

  template<typename T>
  bool compare(T, T);
};

class queryTree {
 public:
  std::vector<std::string> range_table_;
  std::vector<std::string>::size_type result_idx_ = 0;
  std::vector<expr *> target_list_;
  std::vector<expr *> qual_;
  queryTree *left_;
  queryTree *right_;
  bool hasAgg = false;
  bool hasGroup = false;

  queryTree();

  command_type command_;
};

class parser {
 public:
  queryTree stmt_tree_;

  parser() = default;

  void parse(const std::string &);
};

#endif //TOYDB_PARSER_H
