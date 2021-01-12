cmake_minimum_required(VERSION 3.19)
project(lsmtree)

set(CMAKE_CXX_STANDARD 14)

include_directories(src)

add_executable(lsmtree
        src/opt.hpp
        src/skip_list.hpp
        src/bloom_filter.hpp
        main.cpp)
