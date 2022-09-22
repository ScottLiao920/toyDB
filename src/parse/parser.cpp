//
// Created by liaoc on 9/15/22.
//

#include <cstring>
#include "parser.h"
#include "regex"
#include <boost/algorithm/string.hpp>

queryTree::queryTree() {
  this->command_ = INVALID_COMMAND;
//  this->left_ = (queryTree *)std::malloc(sizeof(queryTree));
//  this->right_ = (queryTree *)std::malloc(sizeof(queryTree));
}

void parser::parse(const std::string &sql_string) {
  std::string upp_sql;
  upp_sql = boost::to_upper_copy(sql_string);
  if (upp_sql.compare(0, 6, "SELECT") == 0) {
	this->stmt_tree_.command_ = SELECT;
  } else if (upp_sql.compare(0, 6, "INSERT") == 0) {
	this->stmt_tree_.command_ = INSERT;
  } else if (upp_sql.compare(0, 6, "UPDATE") == 0) {
	this->stmt_tree_.command_ = UPDATE;
  } else if (upp_sql.compare(0, 6, "DELETE") == 0) {
	this->stmt_tree_.command_ = DELETE;
  } else {
	this->stmt_tree_.command_ = INVALID_COMMAND;
	return;
  }
  upp_sql = upp_sql.substr(6);
  std::vector<std::string> TL_RTE; //vector of raw string for target list and range table (optional)
  boost::iter_split(TL_RTE, upp_sql, boost::algorithm::first_finder("FROM"));
  std::vector<std::string> target_list;
  boost::split(target_list, TL_RTE[0], boost::is_any_of(","));
  for (auto &it : target_list) {
	expr *cur_expr = new expr;
	std::memset(cur_expr, 0, sizeof(expr));

	// check for alias
	if (it.find("AS") != std::string::npos) {
	  std::vector<std::string> name_alias;
	  boost::iter_split(name_alias, it, boost::algorithm::first_finder("AS"));
	  assert(name_alias.size() == 2);
	  cur_expr->alias = name_alias[1];
	  boost::erase_all(cur_expr->alias, " ");
	  it = it.substr(0, it.find("AS"));
	}

	// check for aggregation, nested aggregation (MIN(MAX(...) )) currently not supported.
	if (it.find("MIN(") != std::string::npos) {
	  this->stmt_tree_.hasAgg = true;
	  cur_expr->type = AGGR;
	  ((aggr_expr *)cur_expr)->aggr_type = MIN;
	  it = it.substr(it.find("MIN(") + 4);
	  cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
	} else if (it.find("MAX(") != std::string::npos) {
	  this->stmt_tree_.hasAgg = true;
	  cur_expr->type = AGGR;
	  ((aggr_expr *)cur_expr)->aggr_type = MAX;
	  it = it.substr(it.find("MAX(") + 4);
	  cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
	} else if (it.find("AVG(") != std::string::npos) {
	  this->stmt_tree_.hasAgg = true;
	  cur_expr->type = AGGR;
	  ((aggr_expr *)cur_expr)->aggr_type = AVG;
	  it = it.substr(it.find("AVG(") + 4);
	  cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
	} else if (it.find("COUNT(") != std::string::npos) {
	  this->stmt_tree_.hasAgg = true;
	  cur_expr->type = AGGR;
	  ((aggr_expr *)cur_expr)->aggr_type = COUNT;
	  it = it.substr(it.find("COUNT(") + 6);
	  cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
	} else if (it.find("SUM(") != std::string::npos) {
	  this->stmt_tree_.hasAgg = true;
	  cur_expr->type = AGGR;
	  ((aggr_expr *)cur_expr)->aggr_type = SUM;
	  it = it.substr(it.find("SUM(") + 4);
	  cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
	} else {
	  cur_expr->type = COL;
	  cur_expr->data_srcs.emplace_back(it);
	}

	this->stmt_tree_.target_list_.push_back(cur_expr);
  }

  // split remaining SQL statement into range table list and qualifications
  std::vector<std::string> RTE_qual;
  boost::iter_split(RTE_qual, TL_RTE[1], boost::algorithm::first_finder("WHERE"));

  boost::split(this->stmt_tree_.range_table_, RTE_qual[0], boost::is_any_of(","));
  // TODO: lookup a system catalog and map rte name_ to relid
  std::vector<std::string> quals;
  boost::iter_split(quals, RTE_qual[1], boost::algorithm::first_finder("AND"));
  for (const auto &it : quals) {
	// check qualifications in WHERE clause
	expr *cur_qual = new expr;
	std::smatch op;
	std::regex op_regex("[=<>!]");
	std::regex_search(it, op, op_regex);
	if (not op.empty()) {
	  cur_qual->type = COMP;
	  std::string all_op = op.format("$&");
	  std::string first_operand, second_operand;
	  first_operand = op.format("$`");
	  second_operand = op.format("$'");
	  std::remove(first_operand.begin(), first_operand.end(), ' ');
	  std::remove(second_operand.begin(), second_operand.end(), ' ');
	  cur_qual->data_srcs.push_back(first_operand);
	  cur_qual->data_srcs.push_back(second_operand);
	  // check comparison type and assign it accordingly. Maybe have a dict for it?
	  if (all_op == "<=") {
		((comparison_expr *)cur_qual)->comparision_type = ngt;
	  } else if (all_op == "<") {
		((comparison_expr *)cur_qual)->comparision_type = lt;
	  } else if (all_op == ">") {
		((comparison_expr *)cur_qual)->comparision_type = gt;
	  } else if (all_op == ">=") {
		((comparison_expr *)cur_qual)->comparision_type = nlt;
	  } else if (all_op == "=") {
		((comparison_expr *)cur_qual)->comparision_type = equal;
	  } else if (all_op == "!=") {
		((comparison_expr *)cur_qual)->comparision_type = ne;
	  } else {
		((comparison_expr *)cur_qual)->comparision_type = NO_COMP;
	  }
	} else {
	  std::cout << "Parse Error: Qualification List Init Failed." << std::endl;
	}
	this->stmt_tree_.qual_.push_back(cur_qual);
  }
}

expr::expr() {
  alias.reserve(16);
  type = COL;
  data_srcs.reserve(2);
//    std::memset(&alias, 0, 16);
}

expr::~expr() {
//  free(this);
}
