// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/19/22.
//

#include "planner.h"
#include "schema.h"

std::vector<executor *> plan_scan(parseNode *parse_node, bufferPoolManager *buffer_pool_manager);

parseNode generate_scan_node(std::tuple<size_t, size_t, size_t, std::string> data_src) {
  parseNode left_scan_node;
  left_scan_node.type_ = ScanNode;
  left_scan_node.expression_ = new expr;
  left_scan_node.expression_->data_srcs = {data_src};
  left_scan_node.expression_->type = COL;
  return left_scan_node;
}

std::vector<executor *> get_all_join_plans(executor *left,
										   executor *right,
										   bufferPoolManager *buffer_pool_manager) {
  auto nlj1 = new nestedLoopJoinExecutor;
  auto nlj2 = new nestedLoopJoinExecutor;
  nlj1->SetLeft(left);
  nlj1->SetRight(right);
  nlj1->Init();
  nlj1->SetBufferPoolManager(buffer_pool_manager);
  nlj2->SetLeft(right);
  nlj2->SetRight(left);
  nlj2->Init();
  nlj2->SetBufferPoolManager(buffer_pool_manager);
  // Hash join & merge join to be implemented.
  return {nlj1, nlj2};
}

std::vector<executor *> plan_join(parseNode *parse_node, bufferPoolManager *buffer_pool_manager) {
  //TODO;
  std::vector<executor *> out;
  switch (parse_node->child_->type_) {
	case JoinNode: {
	  auto join_plans = plan_join(parse_node, buffer_pool_manager);
	  parseNode scan_node;
	  if (parse_node->expression_->data_srcs[0] == parse_node->child_->expression_->data_srcs[0]
		  || parse_node->expression_->data_srcs[0] == parse_node->child_->expression_->data_srcs[1]) {
		// plan scan on parse_node->expression_->data_srcs[1]
		scan_node = generate_scan_node(parse_node->expression_->data_srcs[1]);
	  } else {
		// plan scan on parse_node->child_->expression_->data_srcs[0]
		scan_node = generate_scan_node(parse_node->expression_->data_srcs[0]);
	  }
	  auto scan_plans = plan_scan(&scan_node, buffer_pool_manager);
	  // combination of join plans and scan plans
	  for (auto it : scan_plans) {
		for (auto it2 : join_plans) {
		  for (auto plan : (get_all_join_plans(it, it2, buffer_pool_manager))) {
			out.push_back(plan);
		  }
		}
	  }
	  break;
	}
	case CompNode: {
	  // Join node follow by a comparison node
	  auto left_scan_node = generate_scan_node(parse_node->expression_->data_srcs[0]);
	  auto right_scan_node = generate_scan_node(parse_node->expression_->data_srcs[1]);
	  auto left_scan_plans = plan_scan(&left_scan_node, buffer_pool_manager);
	  auto right_scan_plans = plan_scan(&right_scan_node, buffer_pool_manager);
	  if (parse_node->expression_->data_srcs[0] == parse_node->child_->expression_->data_srcs[0]) {
		for (auto it : left_scan_plans) {
		  ((scanExecutor *)it)->SetQual((comparison_expr *)parse_node->child_->expression_);
		}
	  } else {
		for (auto it : right_scan_plans) {
		  ((scanExecutor *)it)->SetQual((comparison_expr *)parse_node->child_->expression_);
		}
	  }
	  for (auto it : left_scan_plans) {
		for (auto it2 : right_scan_plans) {
		  for (auto plan : (get_all_join_plans(it, it2, buffer_pool_manager))) {
			out.push_back(plan);
		  }
		}
	  }
	  break;
	}
	case EmptyNode: {
	  // Last node in parse chain, only need to plan scan nodes.
	  auto left_scan_node = generate_scan_node(parse_node->expression_->data_srcs[0]);
	  auto right_scan_node = generate_scan_node(parse_node->expression_->data_srcs[1]);
	  auto left_scan_plans = plan_scan(&left_scan_node, buffer_pool_manager);
	  auto right_scan_plans = plan_scan(&right_scan_node, buffer_pool_manager);
	  for (auto it : left_scan_plans) {
		for (auto it2 : right_scan_plans) {
		  for (auto plan : (get_all_join_plans(it, it2, buffer_pool_manager))) {
			out.push_back(plan);
		  }
		}
	  }
	  break;
	}
	default:break; // Not possible to have other type of nodes after a join node
  }
  return out;
}

std::vector<executor *> plan_scan(parseNode *parse_node, bufferPoolManager *buffer_pool_manager) {
  std::vector<executor *> out;
  auto seqSE = new seqScanExecutor;
  seqSE->Init(table_schema.TableID2Table[std::get<0>(parse_node->expression_->data_srcs[0])],
			  buffer_pool_manager,
			  nullptr);
  out.push_back((executor *)seqSE);
  auto idxSE = new indexScanExecutor;
  out.push_back((executor *)idxSE);
  auto bmiSE = new bitMapIndexScanExecutor;
  out.push_back((executor *)bmiSE);
  for (auto it : out) {
	if (((scanExecutor *)it)->GetTableID() == std::get<0>(parse_node->expression_->data_srcs[0])) {
	  ((scanExecutor *)it)->SetQual((comparison_expr *)parse_node->expression_);
	  break;
	}
  }
  return out;
}

std::vector<executor *> plan_node(parseNode *parse_node, bufferPoolManager *buffer_pool_manager) {
  if (parse_node == nullptr || parse_node->type_ == EmptyNode) {
	return {};
  }
  std::vector<executor *> out;
  switch (parse_node->type_) {
	case AggrNode: {
	  //TODO: this is merely a skeleton
	  auto child_plans = plan_node(parse_node->child_, buffer_pool_manager);
	  for (auto it : child_plans) {
		auto tmp = new aggregateExecutor;
		tmp->child_ = it;
		out.push_back((executor *)tmp);
	  }
	  break;
	}
	case SelectNode: {
	  // buffer all selectNode first
	  std::vector<parseNode *> select_nodes;
	  parseNode *cur_node = parse_node;
	  while (cur_node->type_ == SelectNode) {
		select_nodes.push_back(cur_node);
		cur_node = cur_node->child_;
	  }
	  auto child_plans = plan_node(cur_node, buffer_pool_manager);
	  // Generate select executor's target list
	  std::vector<expr *> target_list;
	  for (auto it : select_nodes) {
		target_list.push_back(it->expression_);
	  }
	  // for every child plan, add all selection clauses on its view.
	  for (auto it : child_plans) {
		auto tmp = new selectExecutor;
		tmp->Init(target_list, {it});
		out.push_back((executor *)tmp);
	  }
	  break;
	}
	case UpdateNode:break;
	case CompNode: { // add qualification to scan executor;
	  return plan_scan(parse_node, buffer_pool_manager);
	}
	case JoinNode: {        // join predicate. add a join operation.
	  return plan_join(parse_node, buffer_pool_manager);
	}
	case EmptyNode:break;
	case ScanNode: { return plan_scan(parse_node, buffer_pool_manager); }
  }
  return out;
}

void planner::plan(queryTree *query_tree) {
  /*
   * This method generate root based on target list, then iteratively go thru all qualifications & generate plan on then recursively
   */
  auto cur_plan = new planTree;
  cur_plan->root = (executor *)new selectExecutor;
  auto out = plan_node(query_tree->root_, this->bpmgr_);
  for (auto tmp : out) {
	((selectExecutor *)cur_plan->root)->addChild(tmp); // TODO: this should contain a complete plan tree
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
