//
// Created by liaoc on 9/15/22.
//

#ifndef TOYDB_PARSER_H
#define TOYDB_PARSER_H

#include "../storage/storage.h"

enum command_type {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    INVALID
};

class expr {

};

class queryTree {
    std::vector<RelID> range_table_;
    std::vector<RelID>::size_type result_idx_ = 0;
    std::vector<expr> target_list_;
    std::vector<expr> qual_;
    queryTree *left_;
    queryTree *right_;
    bool hasAgg = false;
    bool hasGroup = false;


public:
    queryTree();

    command_type command_;
};


class parser {
public:
    queryTree stmt_tree_;

    parser() = default;

    void parse(const std::string &);
};


#endif //TOYDB_PARSER_H
