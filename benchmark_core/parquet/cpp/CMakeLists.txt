# Licensed to the Apache Software Foundation (ASF)
cmake_minimum_required(VERSION 3.16)
project(parquet_bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# nlohmann_json: use system or FetchContent if not found
find_path(JSON_INCLUDE_DIR nlohmann/json.hpp
  HINTS /usr/include /usr/local/include)
if(NOT JSON_INCLUDE_DIR)
  find_package(nlohmann_json 3.2.0 QUIET)
endif()
if(JSON_INCLUDE_DIR)
  include_directories(${JSON_INCLUDE_DIR})
elseif(TARGET nlohmann_json::nlohmann_json)
  # use target
else()
  message(FATAL_ERROR "nlohmann/json not found. Install libnlohmann-json3-dev or nlohmann_json")
endif()

include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(bench_mark_parquet
  src/parquet_bench_main.cc
  src/parquet_bench_utils.cc
)
target_link_libraries(bench_mark_parquet PRIVATE
  Arrow::arrow_shared
  Parquet::parquet_shared
)
