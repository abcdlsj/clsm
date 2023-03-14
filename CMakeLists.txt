cmake_minimum_required(VERSION 3.16)
project(lsmtree)

set(CMAKE_CXX_FLAGS "-Wall -Wextra -O3")

set(CMAKE_CXX_STANDARD 14)

include_directories(src)
find_package (Threads)

add_executable(lsmtree
        src/run.hpp
        src/skip_list.hpp
        src/bloom_filter.hpp
        src/hash_map.hpp
        src/disk_run.hpp
        src/disk_level.hpp
        src/lsm.hpp
        main.cpp)

target_link_libraries (lsmtree ${CMAKE_THREAD_LIBS_INIT})

