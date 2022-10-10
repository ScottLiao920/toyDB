//
// Created by liaoc on 9/19/22.
//

#include "planner.h"

std::vector<executor *> plan_join(expr *expression) {
  //TODO;
  return {};
}

std::vector<executor *> plan_node(expr *expression) {
  std::vector<executor *> out;
  switch (expression->type) {
	case AGGR: {
	  //TODO
	  auto tmp = new aggregateExecutor;
	  out.push_back((executor *)tmp);
	  break;
	}
	case COMP: {
	  if (expression->data_srcs.size() > 1) {
		// join predicate. add a join operation.
		return plan_join(expression);
	  } else {
		// add qualification to scan executor;
		for (auto it : out) {
		  if (((scanExecutor *)it)->GetTable() == expression->data_srcs[0]) {
			((scanExecutor *)it)->SetQual((comparison_expr *)expression);
			break;
		  }
		}
	  }
	  break;
	}
	case COL: {
	  // TODO: should have multiple choices for scan operator. currently those executor does not override virtual methods
	  for (join_type it = nestedLoopJoin; it < hashJoin; it = join_type(it + 1)) {
		auto tmp = new seqScanExecutor;
		out.push_back((executor *)tmp);
	  }
	  break;
	}
	default: break;
  }
}

void planner::plan(queryTree *query_tree) {
  /*
   * This method generate root based on target list, then iteratively go thru all qualifications & generate plan on then recursively
   */
  // TODO:think about how to generate multiple plans at once; This method should call a recursive planning method on each node;
  auto cur_plan = new planTree;
  cur_plan->root = (executor *)new selectExecutor;
  for (auto it : query_tree->target_list_) {
	auto out = plan_node(it);
	for (auto tmp : out) {
	  ((selectExecutor *)cur_plan->root)->addChild(tmp); // TODO: this should contain a complete plan tree
	}
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
