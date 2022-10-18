// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/19/22.
//

#include "planner.h"
#include "schema.h"

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
		  if (((scanExecutor *)it)->GetTableID() == std::get<0>(expression->data_srcs[0])) {
			((scanExecutor *)it)->SetQual((comparison_expr *)expression);
			break;
		  }
		}
	  }
	  break;
	}
	case COL: {
	  // TODO: initialize with enough data
	  // Think: what if this col is from a joined view? Should have a in-memory data structure for tables?
	  auto seqSE = new seqScanExecutor;
	  seqSE->Init(table_schema.TableID2Table[std::get<0>(expression->data_srcs[0])], nullptr, nullptr);
	  out.push_back((executor *)seqSE);
	  auto idxSE = new indexScanExecutor;
	  out.push_back((executor *)idxSE);
	  auto bmiSE = new bitMapIndexScanExecutor;
	  out.push_back((executor *)bmiSE);
	  break;
	}
	default: break;
  }
}

void planner::plan(queryTree *query_tree) {
  /*
   * This method generate root based on target list, then iteratively go thru all qualifications & generate plan on then recursively
   */
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
