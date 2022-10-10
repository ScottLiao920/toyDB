//
// Created by liaoc on 9/19/22.
//

#include "planner.h"

void planner::plan(queryTree *query_tree) {
  // TODO:think about how to generate multiple plans at once; This method should call a recursive planning method on each node;
  auto cur_plan = new planTree;
  cur_plan->root = (executor *)new selectExecutor;
  for (auto it : query_tree->target_list_) {
	switch (it->type) {
	  case AGGR: {
		//TODO
		auto tmp = new aggregateExecutor;
		((selectExecutor *)cur_plan->root)->addChild((executor *)tmp);
		break;
	  }
	  case COL: {
		// TODO: should have multiple choices for scan operator.
		auto tmp = new seqScanExecutor;
		((selectExecutor *)cur_plan->root)->addChild((executor *)tmp);
		break;
	  }
	  default: break;
	}
  }
  for (auto it : query_tree->range_table_) {

  }
}
void planner::execute() {
  for (;;) {
	char indicator[8];
	this->cheapest_tree_->root->Next(indicator);
	if (std::strcmp(indicator, "00000000") != 0) {
	  break;
	}
  }
}
