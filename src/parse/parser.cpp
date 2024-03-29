// Copyright (c) 2022.
// Code written by Liao Chang (cliaosoc@nus.edu.sg)
// Veni, vidi, vici

//
// Created by liaoc on 9/15/22.
//

#include <cassert>
#include "parser.h"
#include "type.h"
#include "schema.h"

typedef expr *pExpr;
queryTree::queryTree() {
  this->command_ = INVALID_COMMAND;
  this->range_table_ = std::vector<std::string>();
//  this->child_ = (queryTree *)std::malloc(sizeof(queryTree));
//  this->right_ = (queryTree *)std::malloc(sizeof(queryTree));
}

bool is_all_digits(std::string string) {
  for (auto c : string) {
	if (std::isdigit(c) == 0) {
	  return false;
	}
  }
  return true;
}

void split(std::string inp, std::string delim, std::vector<std::string> &out) {
  std::regex tmp = std::regex(delim);
  std::sregex_token_iterator it(inp.begin(), inp.end(), tmp, -1);
  std::sregex_token_iterator end;
  for (; it != end; ++it) {
	out.emplace_back(*it);
  }
//  std::copy(it, end, out.begin());
}

std::tuple<size_t, size_t, size_t, std::string> parser::processDataSrc(const std::string &inp) {
  /* If the data src is from a table, return (relID, colID, typeId of the col);
   * else return (INVALID relID, INVALID colID, typeId of the constant).
  */
  // check operand type here. If "." exists in the operand, check for existing tables and columns, if not found,
  // search for float number. Else, should be either a column (without numeric char) or an integer constant.
  std::string raw_data_src(inp);
  raw_data_src.erase(std::remove(raw_data_src.begin(), raw_data_src.end(), ' '), raw_data_src.end());
  std::regex dot("[.]");
  std::smatch tmp;
  std::regex_search(raw_data_src, tmp, dot);
  if (tmp.empty()) {
	if (is_all_digits(raw_data_src)) {
	  return {INVALID_RELID, INVALID_COLID, typeid(std::string).hash_code(), raw_data_src};
	} else {
	  // TODO: search for matching column in all tables.
	}
  } else {
	std::string prefix = tmp.format("$`");
	if (is_all_digits(prefix)) {
	  return {INVALID_RELID, INVALID_COLID, typeid(float).hash_code(), raw_data_src};
	} else {
	  auto it = table_schema.TableName2Table.find(prefix);
	  if (it == table_schema.TableName2Table.end()) {
		// table not found in current schema
		std::cout << "Parse Error: Table " << prefix << " not found in current schema!" << std::endl;
	  } else {
		RelID id = it->second->GetID();
		ColID col_id = it->second->GetColIdx(tmp.format("$'"));
		size_t type_id = it->second->GetTypeIDs()[it->second->GetColIdx(tmp.format("$'"))];
		return {id, col_id, type_id, raw_data_src};
	  }
	}
  }
}

void parser::parse(const std::string &sql_string) {
  if (this->stmt_tree_->root_ != nullptr) {
	this->prev_stmts.emplace_back(this->stmt_tree_);
	this->stmt_tree_ = new queryTree;
  }

  std::string upp_sql = sql_string;
  std::transform(upp_sql.begin(), upp_sql.end(), upp_sql.begin(), ::toupper);
  if (upp_sql.compare(0, 6, "SELECT") == 0) {
	this->stmt_tree_->command_ = SELECT;
	this->stmt_tree_->root_ = new parseNode;
	this->stmt_tree_->root_->type_ = SelectNode;
  } else if (upp_sql.compare(0, 6, "INSERT") == 0) {
	this->stmt_tree_->command_ = INSERT;
	this->stmt_tree_->root_ = new parseNode;
	this->stmt_tree_->root_->type_ = InsertNode;
  } else if (upp_sql.compare(0, 6, "UPDATE") == 0) {
	this->stmt_tree_->command_ = UPDATE;
	this->stmt_tree_->root_ = new parseNode;
	this->stmt_tree_->root_->type_ = UpdateNode;
  } else if (upp_sql.compare(0, 6, "DELETE") == 0) {
	this->stmt_tree_->command_ = DELETE;
	this->stmt_tree_->root_ = new parseNode;
	this->stmt_tree_->root_->type_ = DeleteNode;
  } else if (upp_sql.compare(0, 6, "CREATE") == 0) {
	this->stmt_tree_->command_ = CREATE;
	this->stmt_tree_->root_ = new parseNode;
	this->stmt_tree_->root_->type_ = CreateNode;
  } else if (upp_sql.compare(0, 4, "COPY") == 0) {
	this->stmt_tree_->command_ = COPY;
	this->stmt_tree_->root_ = new parseNode;
	this->stmt_tree_->root_->type_ = CopyNode;
  } else {
	this->stmt_tree_->command_ = INVALID_COMMAND;
	return;
  }
  switch (this->stmt_tree_->command_) {
	case (SELECT): {
	  upp_sql = upp_sql.substr(6);
	  std::smatch TL_RTE; //vector of raw string for target list and range table (optional)
	  std::regex_search(upp_sql, TL_RTE, std::regex("FROM"));
	  std::vector<std::string> target_list;
	  split(std::string(TL_RTE.format("$`")), ",", target_list);
	  parseNode *cur_parse_node = this->stmt_tree_->root_;
	  for (auto &it : target_list) {
		expr *cur_expr = new expr;
		cur_parse_node->expression_ = cur_expr;

		// check for alias
		if (it.find("AS") != std::string::npos) {
		  std::vector<std::string> name_alias;
		  split(it, "AS", name_alias);
		  assert(name_alias.size() == 2);
		  cur_expr->alias = name_alias[1];
		  it = it.substr(0, it.find("AS"));
		} else {
		  // just use input as alias
		  cur_expr->alias = it;
		}
		cur_expr->alias.erase(std::remove(cur_expr->alias.begin(), cur_expr->alias.end(), ' '), cur_expr->alias.end());

		// check for aggregation, nested aggregation (MIN(MAX(...) )) currently not supported.
		cur_parse_node->type_ = AggrNode;
		if (it.find("MIN(") != std::string::npos) {
		  this->stmt_tree_->hasAgg = true;
		  cur_expr->type = AGGR;
		  ((aggr_expr *)cur_expr)->aggr_type = MIN;
		  it = it.substr(it.find("MIN(") + 4);
		  cur_expr->data_srcs.push_back(processDataSrc(it.substr(0, it.find(")"))));
		} else if (it.find("MAX(") != std::string::npos) {
		  this->stmt_tree_->hasAgg = true;
		  cur_expr->type = AGGR;
		  ((aggr_expr *)cur_expr)->aggr_type = MAX;
		  it = it.substr(it.find("MAX(") + 4);
		  cur_expr->data_srcs.push_back(processDataSrc(it.substr(0, it.find(")"))));
		} else if (it.find("AVG(") != std::string::npos) {
		  this->stmt_tree_->hasAgg = true;
		  cur_expr->type = AGGR;
		  ((aggr_expr *)cur_expr)->aggr_type = AVG;
		  it = it.substr(it.find("AVG(") + 4);
		  cur_expr->data_srcs.push_back(processDataSrc(it.substr(0, it.find(")"))));
		} else if (it.find("COUNT(") != std::string::npos) {
		  this->stmt_tree_->hasAgg = true;
		  cur_expr->type = AGGR;
		  ((aggr_expr *)cur_expr)->aggr_type = COUNT;
		  it = it.substr(it.find("COUNT(") + 6);
		  cur_expr->data_srcs.push_back(processDataSrc(it.substr(0, it.find(")"))));
		} else if (it.find("SUM(") != std::string::npos) {
		  this->stmt_tree_->hasAgg = true;
		  cur_expr->type = AGGR;
		  ((aggr_expr *)cur_expr)->aggr_type = SUM;
		  it = it.substr(it.find("SUM(") + 4);
		  cur_expr->data_srcs.push_back(processDataSrc(it.substr(0, it.find(")"))));
		} else {
		  cur_expr->type = COL;
		  cur_parse_node->type_ = SelectNode;
		  cur_expr->data_srcs.emplace_back(processDataSrc(it));
		}
		this->stmt_tree_->target_list_.push_back(cur_expr);
		cur_parse_node->child_ = new parseNode;
		cur_parse_node = cur_parse_node->child_;
	  }

	  // split remaining SQL statement into range table list and qualifications
	  std::vector<std::string> RTE_qual;
	  split(TL_RTE.format("$'"), "WHERE", RTE_qual);
	  std::vector<std::string> tmp_range_table;
	  split(RTE_qual[0], ",", tmp_range_table);
	  this->stmt_tree_->range_table_ = tmp_range_table;
	  if (RTE_qual.size() == 1) {
		// no WHERE clauses
	  } else {
		std::vector<std::string> quals;
		split(RTE_qual[1], "AND", quals);
		for (const auto &it : quals) {
		  // check qualifications in WHERE clause
		  expr *cur_qual = new expr;
		  std::smatch op;
		  std::regex op_regex("[=<>!]+");
		  std::regex_search(it, op, op_regex);
		  if (not op.empty()) {
			cur_qual->type = COMP;
			std::string all_op = op.format("$&");
			std::string first_operand, second_operand;
			first_operand = op.format("$`");
			second_operand = op.format("$'");
			first_operand.erase(std::remove_if(first_operand.begin(),
											   first_operand.end(),
											   [](unsigned char x) { return std::isspace(x); }), first_operand.end());
			second_operand.erase(std::remove_if(second_operand.begin(),
												second_operand.end(),
												[](unsigned char x) { return std::isspace(x); }), second_operand.end());
			cur_qual->data_srcs.push_back(processDataSrc(first_operand));
			cur_qual->data_srcs.push_back(processDataSrc(second_operand));
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
		  this->stmt_tree_->qual_.push_back(cur_qual);
		  cur_parse_node->expression_ = cur_qual;
		  if (std::get<0>(cur_qual->data_srcs[0]) != INVALID_RELID
			  && std::get<0>(cur_qual->data_srcs[1]) != INVALID_RELID) {
			cur_parse_node->type_ = JoinNode;
		  } else {
			cur_parse_node->type_ = CompNode;
		  }
		  cur_parse_node->child_ = new parseNode;
		  cur_parse_node = cur_parse_node->child_;
		}
	  }
	  cur_parse_node->type_ = EmptyNode;

	  /*
		* If the root of query tree is an aggregation node, push its child selection nodes upwards (this only make
		* sense if it's a group by sql statement) or make a new selection node as its parent.
	  */
	  if (this->stmt_tree_->root_->type_ == AggrNode && this->stmt_tree_->hasGroup) {
		// push selection nodes upwards (NOT TESTED)
		cur_parse_node = this->stmt_tree_->root_;
		while (cur_parse_node->child_->type_ == AggrNode) {
		  cur_parse_node = cur_parse_node->child_;
		}
	  } else if (this->stmt_tree_->root_->type_ == AggrNode) {
		cur_parse_node = this->stmt_tree_->root_;
		while (cur_parse_node->type_ == AggrNode) {
		  auto tmp = new parseNode;
		  tmp->type_ = SelectNode;
		  tmp->expression_ = cur_parse_node->expression_;
		  tmp->child_ = this->stmt_tree_->root_;
		  this->stmt_tree_->root_ = tmp;
		  cur_parse_node = cur_parse_node->child_;
		}

	  }
	  break;
	}
	case (CREATE): {
	  // CREATE TABLE TABLE_NAME (COLUMN_NAME dtype, COLUMN_NAME dtype, ...)
	  std::string tab_name = upp_sql.substr(upp_sql.find("TABLE") + 6);
	  tab_name = tab_name.substr(0, tab_name.find('(') - 1);
	  upp_sql = upp_sql.substr(upp_sql.find('(') + 1);
	  std::vector<std::string> buf;
	  split(upp_sql, ",", buf);
	  expr *cur_expr = new expr;
	  cur_expr->type = SCHEMA;
	  cur_expr->alias = std::move(tab_name);
	  for (const auto &it : buf) {
		// Format: COL_NAME dtype others
		std::vector<std::string> out;
		split(it, " ", out);
		std::string col_name = out.at(out.size() - 2);
		std::string dtype_name = out.at(out.size() - 1);
		size_t type_id, width;
		if (std::strcmp(dtype_name.c_str(), "INT") == 0) {
		  type_id = typeid(int).hash_code();
		  width = sizeof(int);
		} else if (std::strcmp(dtype_name.c_str(), "FLOAT") == 0) {
		  type_id = typeid(float).hash_code();
		  width = sizeof(float);
		} else if (std::strncmp(dtype_name.c_str(), "STRING", 6) == 0) {
		  // col_name string(length)
		  type_id = typeid(std::string).hash_code();
		  width = std::stoul(dtype_name.substr(7, -1));
		}
		else if (std::strcmp(dtype_name.c_str(), "SIZE_T") == 0) {
		  type_id = typeid(size_t).hash_code();
		  width = sizeof(size_t);
		}
		cur_expr->data_srcs.emplace_back(INVALID_RELID, width, type_id, col_name);
	  }
	  this->stmt_tree_->root_->expression_ = cur_expr;
	  break;
	}
	case INSERT: {
	  break;
	}
	case COPY: {
	  // COPY table_name from/to file_path(xxx.csv)
	  std::vector<std::string> buf;
	  split(upp_sql, " ", buf);
	  assert(buf.size() == 4);
	  pExpr cur_expr = new expr;
	  cur_expr->type = SCHEMA;
	  cur_expr->alias = buf.at(1);
	  if (buf.at(2) == "FROM") {
		cur_expr->data_srcs.emplace_back(INVALID_RELID, INVALID_COLID, 0, buf.at(buf.size() - 1));
	  } else {
		cur_expr->data_srcs.emplace_back(INVALID_RELID, INVALID_COLID, 1, buf.at(buf.size() - 1));
	  }
	  this->stmt_tree_->root_->expression_ = cur_expr;
	  break;
	}
	case UPDATE:break;
	case DELETE:break;
	case INVALID_COMMAND:break;
  }
}
parser::parser() {
  this->stmt_tree_ = new queryTree;
}

expr::expr() {
  alias.reserve(16);
  type = COL;
//  data_srcs.reserve(2);
//    std::memset(&alias, 0, 16);
}

expr::~expr() {
//  free(this);
}

template<typename T>
bool comparison_expr::compare(T lhs, T rhs) {
  switch (this->comparision_type) {
	case equal: return (lhs == rhs);
	case lt:return (lhs < rhs);
	case ne:return (lhs != rhs);
	case gt:return (lhs > rhs);
	case ngt:return (lhs <= rhs);
	case nlt:return (lhs >= rhs);
	case NO_COMP:return true;
  }
}
bool comparison_expr::compare(const char *lhs_ptr, const char *rhs_ptr, size_t type_id) {
//  std::cout << "Comparing " << (int)*lhs_ptr << " from left side with "
//			<< (int)*rhs_ptr << " from right side" << std::endl;
  switch (type_schema.typeID2type[type_id]) {
	case (SUPPORTED_TYPES::INT): {
	  auto lhs = (int *)lhs_ptr;
	  int rhs = std::strtoll(rhs_ptr, nullptr, 0);
	  return this->compare(*lhs, rhs);
	}
	case (SUPPORTED_TYPES::FLOAT): {
	  auto lhs = (float *)lhs_ptr;
	  auto rhs = (float)std::strtof(rhs_ptr, nullptr);
	  return this->compare(*lhs, rhs);
	}
	case (SUPPORTED_TYPES::SIZE_T): {
	  auto lhs = (size_t *)lhs_ptr;
	  size_t rhs = std::strtoul(rhs_ptr, nullptr, 0);
	  return this->compare(*lhs, rhs);
	}
	case (SUPPORTED_TYPES::STRING): {
	  std::string lhs(lhs_ptr);
	  std::string rhs(rhs_ptr);
	  return this->compare(lhs, rhs);
	}
	default: {
	  std::cout << "Type not supported." << std::endl;
	  return false;
	}
  }
}
unsigned long long makepair(size_t type1, size_t type2) {
  return (type_schema.typeID2type[type1] << 32) + type_schema.typeID2type[type2];
}
bool comparison_expr::compareFunc(char *lhs_ptr, char *rhs_ptr) {
  unsigned long long specifier = makepair(std::get<2>(this->data_srcs[0]), std::get<2>(this->data_srcs[1]));
  switch (specifier) {
	case (0x000000000): {
	  auto lhs = (int *)lhs_ptr;
	  auto rhs = (int *)rhs_ptr;
	  return this->compare(*lhs, *rhs);
	}
	case (0x100000001): {
	  auto lhs = (float *)lhs_ptr;
	  auto rhs = (float *)rhs_ptr;
	  return this->compare(*lhs, *rhs);
	}
	case (0x200000002): {
	  auto lhs = (size_t *)lhs_ptr;
	  auto rhs = (size_t *)rhs_ptr;
	  return this->compare(*lhs, *rhs);
	}
	case (0x300000003): {
	  auto lhs = (int *)lhs_ptr;
	  int rhs = std::strtoll(rhs_ptr, nullptr, 0);
	  return this->compare(*lhs, rhs);
	}
	case (0x000000003): {
	  auto lhs = (int *)lhs_ptr;
	  int rhs = std::strtoul(rhs_ptr, nullptr, 0);
	  return this->compare(*lhs, rhs);
	}
	case (0x100000003): {
	  auto lhs = (float *)lhs_ptr;
	  auto rhs = (float)std::strtof(rhs_ptr, nullptr);
	  return this->compare(*lhs, rhs);
	}
	case (0x200000003): {
	  auto lhs = (size_t *)lhs_ptr;
	  auto rhs = std::strtoul(rhs_ptr, nullptr, 0);
	  return this->compare(*lhs, rhs);
	}
	default: {
	  std::cout << "Comparison not supported for "
				<< specifier << std::endl;
	}
  }
}
