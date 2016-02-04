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

find_package(Threads)

add_library(tpch_common STATIC ${COMMON_SRC})
target_include_directories(tpch_common PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(tpch_common PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories(tpch_common PUBLIC ${Crossbow_INCLUDE_DIRS})
target_link_libraries(tpch_common PUBLIC ${Boost_LIBRARIES})
target_link_libraries(tpch_common PUBLIC crossbow_logger telldb)

set(kuduClient_DIR "/mnt/local/tell/kudu_install/share/kuduClient/cmake")
set(SERVER_SRC
    server/main.cpp
    server/Connection.cpp
    server/Transactions.cpp
)

set(CLIENT_SRC
    client/main.cpp
    client/Client.cpp
    client/CreatePopulate.cpp
)

set(USE_KUDU OFF CACHE BOOL "Build TPC-H additionally for Kudu")
if(${USE_KUDU})
    set(kuduClient_DIR "/usr/local/share/kuduClient/cmake")
    find_package(kuduClient REQUIRED)
    message(STATUS "Kudu header found at ${KUDU_CLIENT_INCLUDE_DIR}")

    add_definitions( -DUSE_KUDU )

    set(SERVER_SRC
        ${SERVER_SRC}
        server/TransactionsKudu.cpp
        server/ConnectionKudu.cpp
    )

    set(CLIENT_SRC
        ${CLIENT_SRC}
        client/CreatePopulateKudu.cpp
    )
endif()

add_executable(tpch_server ${SERVER_SRC})
target_include_directories(tpch_server PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tpch_server PRIVATE tpch_common)
target_link_libraries(tpch_server PRIVATE ${Jemalloc_LIBRARIES})

add_executable(tpch_client ${CLIENT_SRC})
target_include_directories(tpch_client PRIVATE ${Jemalloc_INCLUDE_DIRS})
target_link_libraries(tpch_client PRIVATE tpch_common crossbow_allocator)
target_link_libraries(tpch_client PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(tpch_client PRIVATE ${Jemalloc_LIBRARIES})

if(${USE_KUDU})
    target_include_directories(tpch_server PRIVATE ${KUDU_CLIENT_INCLUDE_DIR})
    target_link_libraries(tpch_server PRIVATE kudu_client)
    target_include_directories(tpch_client PRIVATE ${KUDU_CLIENT_INCLUDE_DIR})
    target_link_libraries(tpch_client PRIVATE kudu_client)
endif()
