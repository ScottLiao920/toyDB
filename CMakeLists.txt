cmake_minimum_required(VERSION 3.22)
project(toyDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -Wextra -g -O0 -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
#set(CMAKE_CXX_FLAGS_DEBUG "-g")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(src/include /usr/lib/x86_64-linux-gnu)
set(SOURCE_FILES main.cpp src/storage/storage.cpp src/include/storage.h src/include/bufferpool.h src/bufferpool/bufferpool.cpp
        src/parse/parser.cpp src/include/parser.h src/planner/planner.cpp src/include/planner.h src/include/common.h
        src/include/executor.h src/executor/executor.cpp src/include/btree.h src/utils/btree.cpp src/include/config.h src/include/type.h src/include/schema.h src/executor/NestedLoopJoin.cpp src/executor/SequentialScan.cpp src/executor/Select.cpp src/executor/Aggregations.cpp src/executor/hashJoin.cpp)
add_executable(toyDB ${SOURCE_FILES})
