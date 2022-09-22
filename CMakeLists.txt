cmake_minimum_required(VERSION 3.22)
project(toyDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

include_directories(src/include /usr/lib/x86_64-linux-gnu)
add_executable(toyDB main.cpp src/storage/storage.cpp src/include/storage.h src/bufferpool/bufferpool.cpp src/parse/parser.cpp src/include/parser.h src/planner/planner.cpp src/include/planner.h src/include/common.h src/include/executor.h src/executor/executor.cpp)
