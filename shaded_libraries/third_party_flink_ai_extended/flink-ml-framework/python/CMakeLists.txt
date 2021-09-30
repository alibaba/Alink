cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
set(PYBIND11_CPP_STANDARD -std=c++11)

set(CMAKE_VERBOSE_MAKEFILE ON)

project(flink_ml_framework)

set(SRCDIR "${PROJECT_SOURCE_DIR}/flink_ml_framework")
include_directories(${PROJECT_SOURCE_DIR}/flink_ml_framework/include)
set(NATIVE_FILES "${SRCDIR}/include/spscqueue.h"  "${SRCDIR}/include/java_file.h")
set(PYBIND_FILES "${SRCDIR}/ops/java_file_python_binding.cc")

message (${NATIVE_FILES})
add_subdirectory(lib/pybind11-2.2.4)

find_package(Threads REQUIRED)

add_library(nativeimpl OBJECT ${NATIVE_FILES} flink_ml_framework/include/rw_test.h)

message(STATUS "####### ${PYBIND_FILES}")
add_library(java_file_c SHARED ${PYBIND_FILES} $<TARGET_OBJECTS:nativeimpl>)
set_target_properties(java_file_c PROPERTIES
					CXX_VISIBILITY_PRESET "hidden"
					INTERPROCEDURAL_OPTIMIZATION TRUE
					PREFIX "${PYTHON_MODULE_PREFIX}"
                                        SUFFIX "${PYTHON_MODULE_EXTENSION}")
target_link_libraries(java_file_c PRIVATE pybind11::module Threads::Threads)

