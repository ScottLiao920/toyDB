//
// Created by liaoc on 9/15/22.
//

#include <cstring>
#include "parser.h"
#include "regex"
#include <boost/algorithm/string.hpp>

queryTree::queryTree() {
    this->command_ = INVALID_COMMAND;
    this->left_ = (queryTree *) std::malloc(sizeof(queryTree));
    this->right_ = (queryTree *) std::malloc(sizeof(queryTree));
}

void parser::parse(const std::string &sql_string) {
    std::string upp_sql;
    upp_sql = boost::to_upper_copy(sql_string);
    if (upp_sql.compare(0, 6, "SELECT") == 0) {
        this->stmt_tree_.command_ = SELECT;
    }
    else if (upp_sql.compare(0, 6, "INSERT") == 0) {
        this->stmt_tree_.command_ = INSERT;
    }
    else if (upp_sql.compare(0, 6, "UPDATE") == 0) {
        this->stmt_tree_.command_ = UPDATE;
    }
    else if (upp_sql.compare(0, 6, "DELETE") == 0) {
        this->stmt_tree_.command_ = DELETE;
    }
    else {
        this->stmt_tree_.command_ = INVALID_COMMAND;
        return;
    }
    upp_sql = upp_sql.substr(6);
    std::vector<std::string> tmp;
    iter_split(tmp, upp_sql, boost::algorithm::first_finder("FROM"));
    std::vector<std::string> target_list;
    boost::split(target_list, tmp[0], boost::is_any_of(","));
    for (auto &it: target_list) {
        expr *cur_expr = (expr *) malloc(sizeof(expr));

        // check for aggregation, nested aggregation (MIN(MAX(...) )) currently not supported.
        if (it.find("MIN(") != std::string::npos) {
            this->stmt_tree_.hasAgg = true;
            cur_expr->type = AGGR;
            ((aggr_expr *) cur_expr)->aggr_type = MIN;
            it = it.substr(it.find("MIN("));
            cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
        }
        else if (it.find("MAX(") != std::string::npos) {
            this->stmt_tree_.hasAgg = true;
            cur_expr->type = AGGR;
            ((aggr_expr *) cur_expr)->aggr_type = MAX;
            it = it.substr(it.find("MAX("));
            cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
        }
        else if (it.find("AVG(") != std::string::npos) {
            this->stmt_tree_.hasAgg = true;
            cur_expr->type = AGGR;
            ((aggr_expr *) cur_expr)->aggr_type = AVG;
            it = it.substr(it.find("AVG("));
            cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
        }
        else if (it.find("COUNT(") != std::string::npos) {
            this->stmt_tree_.hasAgg = true;
            cur_expr->type = AGGR;
            ((aggr_expr *) cur_expr)->aggr_type = COUNT;
            it = it.substr(it.find("COUNT("));
            cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
        }
        else if (it.find("SUM(") != std::string::npos) {
            this->stmt_tree_.hasAgg = true;
            cur_expr->type = AGGR;
            ((aggr_expr *) cur_expr)->aggr_type = SUM;
            it = it.substr(it.find("SUM("));
            cur_expr->data_srcs.push_back(it.substr(0, it.find(")")));
        }

        // check for alias
        if (it.find("AS") != std::string::npos) {
            std::vector<std::string> alias;
            iter_split(alias, it, boost::algorithm::first_finder("AS"));
            assert(alias.size() == 2);
            cur_expr->alias = alias[1];
        }

        this->stmt_tree_.target_list_.push_back(cur_expr);
    }
}

expr::expr() = default;

expr::~expr() {
    free(this);
}
