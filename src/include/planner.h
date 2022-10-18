// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/19/22.
//

#ifndef TOYDB_PLANNER_H
#define TOYDB_PLANNER_H

#include "common.h"
#include "parser.h"
#include "executor.h"

struct planTree {
  executor *root;
};

class planner {
 private:
  std::vector<std::tuple<planTree *, size_t>> trees;
  planTree *cheapest_tree_;
  bufferPoolManager bpmgr_;
 public:
  friend class parser;
  planner() = default;
  void plan(queryTree *);
  void execute();
};

#endif //TOYDB_PLANNER_H
