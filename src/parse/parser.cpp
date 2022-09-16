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
    std::vector<std::string> TL_RTE; //vector of raw string for target list and range table (optional)
    iter_split(TL_RTE, upp_sql, boost::algorithm::first_finder("FROM"));
    std::vector<std::string> target_list;
    boost::split(target_list, TL_RTE[0], boost::is_any_of(","));
    for (auto &it: target_list) {
        expr *cur_expr = (expr *) malloc(sizeof(expr));

        // check for alias
        if (it.find("AS") != std::string::npos) {
            std::vector<std::string> name_alias;
            iter_split(name_alias, it, boost::algorithm::first_finder("AS"));
            assert(name_alias.size() == 2);
            strcpy(reinterpret_cast<char *>(&cur_expr->alias), reinterpret_cast<const char *>(&name_alias[1]));
//            cur_expr->alias += name_alias[1];
            it = it.substr(0, it.find("AS"));
        }

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
        else {
            cur_expr->type = COL;
        }

        this->stmt_tree_.target_list_.push_back(cur_expr);
    }
}

expr::expr() = default;

expr::~expr() {
    free(this);
}
