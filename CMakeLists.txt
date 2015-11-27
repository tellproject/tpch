cmake_minimum_required(VERSION 3.0.2)
project(tpch)

# Set CMake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

find_package(Boost REQUIRED COMPONENTS system date_time)
find_package(TellDB REQUIRED)
find_package(Crossbow REQUIRED)
find_package(Jemalloc REQUIRED)

set(COMMON_SRC
    common/Protocol.cpp
    common/Util.cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -mcx16")

add_library(tpch_common STATIC ${COMMON_SRC})
target_include_directories(tpch_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(tpch_common PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(tpch_common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tpch_common PUBLIC ${Boost_LIBRARIES})
target_link_libraries(tpch_common PUBLIC telldb)

set(DB_GEN_SRC
    server/dbgen/driver.c
    server/dbgen/build.c
    server/dbgen/bm_utils.c
    server/dbgen/rnd.c
    server/dbgen/print.c
    server/dbgen/load_stub.c
    server/dbgen/bcd2.c
    server/dbgen/speed_seed.c
    server/dbgen/text.c
    server/dbgen/permute.c
    server/dbgen/rng64.c)

set(SERVER_SRC
    server/main.cpp
    server/Connection.cpp
    server/Populate.cpp
    server/CreateSchema.cpp)

set(CLIENT_SRC
    client/main.cpp
    client/Client.cpp)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/server/ch-tables/nation.tbl ${CMAKE_CURRENT_BINARY_DIR}/ch-tables/nation.tbl COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/server/ch-tables/region.tbl ${CMAKE_CURRENT_BINARY_DIR}/ch-tables/region.tbl COPYONLY)

add_library(dbgen ${DB_GEN_SRC})
target_link_libraries(dbgen m)

add_executable(tpch_server ${SERVER_SRC})
target_link_libraries(tpch_server PRIVATE tpch_common dbgen)

# Link against Jemalloc
target_include_directories(tpch_server PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tpch_server PRIVATE ${Jemalloc_LIBRARIES})

add_executable(tpch_client ${CLIENT_SRC})
target_link_libraries(tpch_client PRIVATE tpch_common)

# Link against Jemalloc
target_include_directories(tpch_client PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tpch_client PRIVATE ${Jemalloc_LIBRARIES})
