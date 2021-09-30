cmake_minimum_required(VERSION 3.6)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")
set(PYTHON_LIB_SUFFIX ".so")

set(CMAKE_VERBOSE_MAKEFILE ON)

project(flink_ml_tensorflow)

#add cmake modules
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/modules)
find_package(TensorFlow REQUIRED)
find_package(Threads REQUIRED)

set(MEM_QUEUE_DIR "${PROJECT_SOURCE_DIR}/flink_ml_tensorflow/include/")
include_directories("${MEM_QUEUE_DIR}")
set(SRCDIR "${PROJECT_SOURCE_DIR}/flink_ml_tensorflow/ops/")

set(NATIVE_FILES "${MEM_QUEUE_DIR}/spscqueue.h"
		"${SRCDIR}/queue_file_system.h"
		"${SRCDIR}/queue_file_system.cc"
		"${SRCDIR}/flink_writer_ops.cc")

message (${NATIVE_FILES})

add_library(flink_ops SHARED ${NATIVE_FILES})
set_target_properties(flink_ops PROPERTIES  SUFFIX "${PYTHON_LIB_SUFFIX}")
target_include_directories(flink_ops PUBLIC ${SRCDIR} ${TensorFlow_INCLUDE_DIR})
target_link_libraries(flink_ops ${TensorFlow_LIBRARY} Threads::Threads)

