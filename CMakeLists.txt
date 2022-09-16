cmake_minimum_required(VERSION 3.23)
project(toyDB)

set(CMAKE_CXX_STANDARD 17)

add_executable(toyDB main.cpp src/storage/storage.cpp src/storage/storage.h src/bufferpool/bufferpool.cpp src/parse/parser.cpp src/parse/parser.h)
