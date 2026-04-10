cmake_minimum_required(VERSION 3.10)

project(ccbench_aria
        VERSION 0.0.1
        DESCRIPTION "aria of ccbench"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/../cmake")

option(ENABLE_SANITIZER "enable sanitizer on debug build" ON)
option(ENABLE_UB_SANITIZER "enable undefined behavior sanitizer on debug build" OFF)
option(ENABLE_COVERAGE "enable coverage on debug build" OFF)

find_package(Doxygen)
find_package(Threads REQUIRED)
find_package(gflags REQUIRED)
find_package(glog REQUIRED)
find_package(Boost
        COMPONENTS filesystem)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CompileOptions)

file(GLOB ARIA_SOURCES
        "../common/result.cc"
        "../common/util.cc"
        "result.cc"
        "aria.cc"
        "transaction.cc"
        "util.cc"
        )

add_executable(aria.exe ${ARIA_SOURCES})

set(ARIA_LINK_LIBS
        Boost::filesystem
        gflags::gflags
        Threads::Threads
        )

if (EXISTS ${PROJECT_SOURCE_DIR}/../third_party/mimalloc/out/release/libmimalloc.a)
    list(APPEND ARIA_LINK_LIBS
            ${PROJECT_SOURCE_DIR}/../third_party/mimalloc/out/release/libmimalloc.a)
endif ()

if (EXISTS ${PROJECT_SOURCE_DIR}/../third_party/masstree/libkohler_masstree_json.a)
    list(APPEND ARIA_LINK_LIBS
            ${PROJECT_SOURCE_DIR}/../third_party/masstree/libkohler_masstree_json.a)
endif ()

target_link_libraries(aria.exe ${ARIA_LINK_LIBS})

if (DEFINED ADD_ANALYSIS)
    add_definitions(-DADD_ANALYSIS=${ADD_ANALYSIS})
else ()
    add_definitions(-DADD_ANALYSIS=0)
endif ()

if (DEFINED KEY_SIZE)
    add_definitions(-DKEY_SIZE=${KEY_SIZE})
else ()
    add_definitions(-DKEY_SIZE=8)
endif ()

if (DEFINED MASSTREE_USE)
    add_definitions(-DMASSTREE_USE=${MASSTREE_USE})
else ()
    add_definitions(-DMASSTREE_USE=0)
endif ()

if (DEFINED PARTITION_TABLE)
    add_definitions(-DPARTITION_TABLE=${PARTITION_TABLE})
else ()
    add_definitions(-DPARTITION_TABLE=0)
endif ()

if (DEFINED PROCEDURE_SORT)
    add_definitions(-DPROCEDURE_SORT=${PROCEDURE_SORT})
else ()
    add_definitions(-DPROCEDURE_SORT=0)
endif ()

if (DEFINED VAL_SIZE)
    add_definitions(-DVAL_SIZE=${VAL_SIZE})
else ()
    add_definitions(-DVAL_SIZE=4)
endif ()
