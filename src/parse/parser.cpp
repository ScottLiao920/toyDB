//
// Created by liaoc on 9/15/22.
//

#include <cstring>
#include "parser.h"
#include "regex"
#include <boost/algorithm/string.hpp>

queryTree::queryTree() {
    this->command_ = INVALID;
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
        this->stmt_tree_.command_ = INVALID;
    }

//    boost::split();
}
