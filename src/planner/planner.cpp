// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/19/22.
//

#include "planner.h"

#include <utility>
#include "schema.h"

std::vector<executor *> plan_scan(parseNode *parse_node, bufferPoolManager *buffer_pool_manager);

parseNode generate_scan_node(std::tuple<size_t, size_t, size_t, std::string> data_src) {
  parseNode left_scan_node;
  left_scan_node.type_ = ScanNode;
  left_scan_node.expression_ = new expr;
  left_scan_node.expression_->data_srcs = {std::move(data_src)};
  left_scan_node.expression_->type = COL;
  return left_scan_node;
}

std::vector<executor *> get_all_join_plans(executor *left,
										   executor *right,
										   bufferPoolManager *buffer_pool_manager,
										   comparison_expr *join_predicate) {
  auto nlj1 = new nestedLoopJoinExecutor;
  auto nlj2 = new nestedLoopJoinExecutor;
  nlj1->SetLeft(left);
  nlj1->SetRight(right);
  nlj1->SetBufferPoolManager(buffer_pool_manager);
  nlj1->SetPredicate(join_predicate);
//  nlj1->Init(); // Init will trigger Next() of left & right child

  nlj2->SetLeft(right);
  nlj2->SetRight(left);
  nlj2->SetBufferPoolManager(buffer_pool_manager);
  nlj2->SetPredicate(join_predicate);
//  nlj2->Init(); // Init will trigger Next() of left & right child
  // Hash join & merge join to be implemented.
  return {nlj1, nlj2};
}

std::vector<executor *> plan_join(parseNode *parse_node, bufferPoolManager *buffer_pool_manager) {
  //TODO;
  std::vector<executor *> out;
  switch (parse_node->child_->type_) {
	case JoinNode: {
	  auto join_plans = plan_join(parse_node->child_, buffer_pool_manager);
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
		  for (auto plan : (get_all_join_plans(it,
											   it2,
											   buffer_pool_manager,
											   (comparison_expr *)parse_node->expression_))) {
			out.push_back(plan);
		  }
		}
	  }
	  break;
	}
	case CompNode: {
	  // Join node follow by comparison nodes. It's parser's job to ensure no other types of nodes following it.
	  auto left_scan_node = generate_scan_node(parse_node->expression_->data_srcs[0]);
	  auto right_scan_node = generate_scan_node(parse_node->expression_->data_srcs[1]);
	  auto left_scan_plans = plan_scan(&left_scan_node, buffer_pool_manager);
	  auto right_scan_plans = plan_scan(&right_scan_node, buffer_pool_manager);

	  // Plan tailing comparison nodes
	  parseNode *cur_node = parse_node->child_;
	  while (cur_node->type_ != EmptyNode) {
		if (parse_node->expression_->data_srcs[0] == cur_node->expression_->data_srcs[0]) {
		  for (auto it : left_scan_plans) {
			((scanExecutor *)it)->AddQual((comparison_expr *)cur_node->expression_);
		  }
		} else {
		  for (auto it : right_scan_plans) {
			((scanExecutor *)it)->AddQual((comparison_expr *)cur_node->expression_);
		  }
		}
		cur_node = cur_node->child_;
	  }
	  for (auto it : left_scan_plans) {
		for (auto it2 : right_scan_plans) {
		  for (auto plan : (get_all_join_plans(it,
											   it2,
											   buffer_pool_manager,
											   (comparison_expr *)parse_node->expression_))) {
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
		  for (auto plan : (get_all_join_plans(it,
											   it2,
											   buffer_pool_manager,
											   (comparison_expr *)parse_node->expression_))) {
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
  seqSE->SetTable(table_schema.TableID2Table[std::get<0>(parse_node->expression_->data_srcs[0])]);
  seqSE->SetBufferPoolManager(buffer_pool_manager);
//  seqSE->Init(table_schema.TableID2Table[std::get<0>(parse_node->expression_->data_srcs[0])],
//			  buffer_pool_manager,
//			  nullptr);
  out.push_back((executor *)seqSE);
//  auto idxSE = new indexScanExecutor;
//  out.push_back((executor *)idxSE);
//  auto bmiSE = new bitMapIndexScanExecutor;
//  out.push_back((executor *)bmiSE);
  for (auto it : out) {
	if (((scanExecutor *)it)->GetTableID() == std::get<0>(parse_node->expression_->data_srcs[0])) {
	  ((scanExecutor *)it)->AddQual((comparison_expr *)parse_node->expression_);
	}
  }
  return out;
}

std::vector<executor *> plan_node(parseNode *parse_node, bufferPoolManager *buffer_pool_manager) {
  // Should an executor's data_src located at its child executor's view?
  if (parse_node == nullptr || parse_node->type_ == EmptyNode) {
	return {};
  }
  std::vector<executor *> out;
  switch (parse_node->type_) {
	case AggrNode: {
	  //TODO: It should plan non-aggregation nodes first?
	  std::vector<parseNode *> aggr_nodes;
	  parseNode *cur_node = parse_node;
	  while (cur_node->type_ == AggrNode) {
		aggr_nodes.push_back(cur_node);
		cur_node = cur_node->child_;
	  }
	  auto child_plans = plan_node(cur_node, buffer_pool_manager);
	  // Need to verify the data source to feed into SetColumn (should be raw column name)
	  for (auto it : child_plans) {
		switch (((aggr_expr *)parse_node->expression_)->aggr_type) {
		  case MIN: {
			auto tmp = new MinAggregateExecutor;
			tmp->SetBufferPoolManager(buffer_pool_manager);
			tmp->SetChild(it);
			tmp->SetColumn(std::get<3>(parse_node->expression_->data_srcs[0]));
			out.push_back(tmp);
			break;
		  };
		  case MAX: {
			auto tmp = new MaxAggregateExecutor;
			tmp->SetBufferPoolManager(buffer_pool_manager);
			tmp->SetChild(it);
			tmp->SetColumn(std::get<3>(parse_node->expression_->data_srcs[0]));
			out.push_back(tmp);
			break;
		  };
		  case COUNT: {
			auto tmp = new CountAggregateExecutor;
			tmp->SetBufferPoolManager(buffer_pool_manager);
			tmp->SetChild(it);
			tmp->SetColumn(std::get<3>(parse_node->expression_->data_srcs[0]));
			out.push_back(tmp);
			break;
		  }
		  case AVG: {
			auto tmp = new AvgAggregateExecutor;
			tmp->SetBufferPoolManager(buffer_pool_manager);
			tmp->SetChild(it);
			tmp->SetColumn(std::get<3>(parse_node->expression_->data_srcs[0]));
			out.push_back(tmp);
			break;
		  }
		  case SUM: {
			auto tmp = new SumAggregateExecutor;
			tmp->SetBufferPoolManager(buffer_pool_manager);
			tmp->SetChild(it);
			tmp->SetColumn(std::get<3>(parse_node->expression_->data_srcs[0]));
			out.push_back(tmp);
			break;
		  }
		  case NO_AGGR:break;
		}
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
		tmp->addChild(it);
		for (auto target : target_list) {
		  tmp->addTarget(target);
		}
//		tmp->Init(target_list, {it});
		out.push_back((executor *)tmp);
	  }
	  break;
	}
	case UpdateNode: {
	  break;
	}
	case CompNode: { // add qualification to scan executor;
	  return plan_scan(parse_node, buffer_pool_manager);
	}
	case JoinNode: {        // join predicate. add a join operation.
	  return plan_join(parse_node, buffer_pool_manager);
	}
	case EmptyNode:break;
	case ScanNode: { return plan_scan(parse_node, buffer_pool_manager); }
	case CreateNode: {
	  auto create_exec = new createExecutor;
	  ((executor*)create_exec)->SetBufferPoolManager(buffer_pool_manager);
	  create_exec->SetName(parse_node->expression_->alias);
	  create_exec->SetCols(parse_node->expression_->data_srcs);
	  out.push_back((executor *)create_exec);
	}
	case InsertNode:break;
  }
  return out;
}

void planner::plan(queryTree *query_tree) {
  /*
   * This method generate root based on target list, then iteratively go thru all qualifications & generate plan on them recursively
   */
  if (not this->trees.empty()) {
	this->prev_trees.insert(this->prev_trees.end(), std::make_move_iterator(this->trees.begin()),
							std::make_move_iterator(this->trees.end()));
	this->trees.clear();
  }
  auto out = plan_node(query_tree->root_, this->bpmgr_);
  size_t cur_min = 0xFFFFFFFF;
  for (auto tmp : out) {
	auto cur_plan = new planTree;
	cur_plan->root = tmp;
	this->trees.emplace_back(cur_plan, 0xFFFFFFFF);
	this->cheapest_tree_ = cur_plan;
  }
}
void planner::execute() {
  char stopper[8];
  std::memset(stopper, 0, 8);
  while (true) {
	char indicator[8];
	this->cheapest_tree_->root->Next(indicator);
	if (std::memcmp(indicator, stopper, 8) == 0) {
	  break;
	}
  }
  this->cheapest_tree_->root->End();
}
void planner::Init() {
  this->cheapest_tree_->root->Init();
}
