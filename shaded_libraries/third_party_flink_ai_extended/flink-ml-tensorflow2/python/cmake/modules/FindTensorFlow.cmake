# Patrick Wieschollek, <mail@patwie.com>
# FindTensorFlow.cmake
# https://github.com/PatWie/tensorflow-cmake/blob/master/cmake/modules/FindTensorFlow.cmake
# -------------
#
# Find TensorFlow library and includes
#
# Automatically set variables have prefix "TensorFlow_",
# while environmental variables you can specify have prefix "TENSORFLOW_"
# This module will set the following variables in your project:
#
# ``TensorFlow_VERSION``
#   exact TensorFlow version obtained from runtime
# ``TensorFlow_ABI``
#   ABI specification of TensorFlow library obtained from runtime
# ``TensorFlow_INCLUDE_DIR``
#   where to find tensorflow header files obtained from runtime
# ``TensorFlow_LIBRARY``
#   the libraries to link against to use TENSORFLOW obtained from runtime
# ``TensorFlow_FOUND TRUE``
#   If false, do not try to use TENSORFLOW.
# ``TensorFlow_C_LIBRARY``
#   Path to tensorflow_cc library (libtensorflow[.so,.dylib,.dll], or similar)
#
#  for some examples, you will need to specify on of the following cmake variables:
# ``TensorFlow_BUILD_DIR`` Is the directory containing the tensorflow_cc library, which can be initialized
#  with env-var 'TENSORFLOW_BUILD_DIR' environmental variable
# ``TensorFlow_SOURCE_DIR`` Is the path to source of TensorFlow, which can be initialized
#  with env-var 'TENSORFLOW_SOURCE_DIR' environmental variable
#
#
# USAGE
# ------
# add "list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}../../path/to/this/file)" to your project
#
# "add_tensorflow_gpu_operation" is a macro to compile a custom operation
#
# add_tensorflow_gpu_operation("<op-name>") expects the following files to exists:
#   - kernels/<op-name>_kernel.cc
#   - kernels/<op-name>_kernel_gpu.cu.cc (kernels/<op-name>_kernel.cu is supported as well)
#   - kernels/<op-name>_op.cc
#   - kernels/<op-name>_op.h
#   - ops/<op-name>.cc

if(APPLE)
  message(WARNING "This FindTensorflow.cmake is not tested on APPLE\n"
                  "Please report if this works\n"
                  "https://github.com/PatWie/tensorflow-cmake")
endif()

if(WIN32)
  message(WARNING "This FindTensorflow.cmake is not tested on WIN32\n"
                  "Please report if this works\n"
                  "https://github.com/PatWie/tensorflow-cmake")
endif()

set(PYTHON_EXECUTABLE "python3" CACHE STRING "specify the python version TensorFlow is installed on.")

if(TensorFlow_FOUND AND EXISTS "${TensorFlow_LIBRARY}" AND IS_DIRECTORY "${TensorFlow_INCLUDE_DIR}")
  # reuse cached variables
  message(STATUS "Reuse cached information from TensorFlow ${TensorFlow_VERSION} ")
else()
  message(STATUS "Detecting TensorFlow using ${PYTHON_EXECUTABLE}"
          " (use -DPYTHON_EXECUTABLE=... otherwise)")
  execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import tensorflow as tf; print(tf.__version__); print(tf.__cxx11_abi_flag__); print(tf.sysconfig.get_include()); print(tf.sysconfig.get_lib());"
    OUTPUT_VARIABLE TF_INFORMATION_STRING
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE retcode)

  if(NOT "${retcode}" STREQUAL "0")
    message(FATAL_ERROR "Detecting TensorFlow info - failed  \n Did you installed TensorFlow?")
  else()
    message(STATUS "Detecting TensorFlow info - done")
  endif()

  string(REPLACE "\n" ";" TF_INFORMATION_LIST ${TF_INFORMATION_STRING})
  list(GET TF_INFORMATION_LIST 0 TF_DETECTED_VERSION)
  list(GET TF_INFORMATION_LIST 1 TF_DETECTED_ABI)
  list(GET TF_INFORMATION_LIST 2 TF_DETECTED_INCLUDE_DIR)
  list(GET TF_INFORMATION_LIST 3 TF_DETECTED_LIBRARY_PATH)

  # set(TF_DETECTED_VERSION 1.8)

  set(_packageName "TF")
  if (DEFINED TF_DETECTED_VERSION)
      string (REGEX MATCHALL "[0-9]+" _versionComponents "${TF_DETECTED_VERSION}")
      list (LENGTH _versionComponents _len)
      if (${_len} GREATER 0)
          list(GET _versionComponents 0 TF_DETECTED_VERSION_MAJOR)
      endif()
      if (${_len} GREATER 1)
          list(GET _versionComponents 1 TF_DETECTED_VERSION_MINOR)
      endif()
      if (${_len} GREATER 2)
          list(GET _versionComponents 2 TF_DETECTED_VERSION_PATCH)
      endif()
      if (${_len} GREATER 3)
          list(GET _versionComponents 3 TF_DETECTED_VERSION_TWEAK)
      endif()
      set (TF_DETECTED_VERSION_COUNT ${_len})
  else()
      set (TF_DETECTED_VERSION_COUNT 0)
  endif()


  # -- prevent pre 1.9 versions
  # Note: TensorFlow 1.7 supported custom ops and all header files.
  # TensorFlow 1.8 broke that promise and 1.9, 1.10 are fine again.
  # This cmake-file is only tested against 1.9+.
  if("${TF_DETECTED_VERSION}" VERSION_LESS "1.9")
    message(FATAL_ERROR "Your installed TensorFlow version ${TF_DETECTED_VERSION} is too old.")
  endif()

  if(TF_FIND_VERSION_EXACT)
    # User requested exact match of TensorFlow.
    # TensorFlow release cycles are currently just depending on (major, minor)
    # But we test against both.
    set(_TensorFlow_TEST_VERSIONS
        "${TF_FIND_VERSION_MAJOR}.${TF_FIND_VERSION_MINOR}.${TF_FIND_VERSION_PATCH}"
        "${TF_FIND_VERSION_MAJOR}.${TF_FIND_VERSION_MINOR}")
  else() # TF_FIND_VERSION_EXACT
    # User requested not an exact TensorFlow version.
    # However, only TensorFlow versions 1.9, 1.10 support all header files
    # for custom ops.
    set(_TensorFlow_KNOWN_VERSIONS ${TensorFlow_ADDITIONAL_VERSIONS}
        "1.9" "1.9.0" "1.10" "1.10.0" "1.11" "1.11.0" "1.12" "1.12.0" "1.13" "1.13.1" "1.15" "2.3")
    set(_TensorFlow_TEST_VERSIONS)

    if(TF_FIND_VERSION)
        set(_TF_FIND_VERSION_SHORT "${TF_FIND_VERSION_MAJOR}.${TF_FIND_VERSION_MINOR}")
        # Select acceptable versions.
        foreach(version ${_TensorFlow_KNOWN_VERSIONS})
          if(NOT "${version}" VERSION_LESS "${TF_FIND_VERSION}")
            # This version is high enough.
            list(APPEND _TensorFlow_TEST_VERSIONS "${version}")
          endif()
        endforeach()
      else() # TF_FIND_VERSION
        # Any version is acceptable.
        set(_TensorFlow_TEST_VERSIONS "${_TensorFlow_KNOWN_VERSIONS}")
      endif()
  endif()

  #### ---- Configure TensorFlow_SOURCE_DIR
  # Order of precidence is 1) CMake variable value, 2) Environmental Variable value
  if(IS_DIRECTORY "${TensorFlow_SOURCE_DIR}")
    set(TensorFlow_SOURCE_DIR "${TensorFlow_SOURCE_DIR}" CACHE PATH "directory containing the file 'libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}'")
  else()
    if(IS_DIRECTORY "$ENV{TENSORFLOW_SOURCE_DIR}")
      set(TensorFlow_SOURCE_DIR "$ENV{TENSORFLOW_SOURCE_DIR}" CACHE PATH "source code for tensorflow (i.e. the git checkout directory of the source code)")
    else()
      set(TensorFlow_SOURCE_DIR "TensorFlow_SOURCE_DIR-NOTFOUND" CACHE PATH "source code for tensorflow (i.e. the git checkout directory of the source code)")
    endif()
  endif()

  # Report on status of cmake cache variable for TensorFlow_SOURCE_DIR
  if(IS_DIRECTORY ${TensorFlow_SOURCE_DIR})
    message(STATUS "TensorFlow_SOURCE_DIR is ${TensorFlow_SOURCE_DIR}")
  else()
    # NOTE This is not a fatal error for backward compatibility ("custom_op test")
    message(STATUS "No directory at 'TensorFlow_SOURCE_DIR:PATH=${TensorFlow_SOURCE_DIR}' detected,\n"
      "please specify the path in ENV 'export TENSORFLOW_SOURCE_DIR=...'\n or cmake -DTensorFlow_SOURCE_DIR:PATH=...\n"
      "to the directory containing the source code for tensorflow\n (i.e. the git checkout directory of the source code)"
      )
  endif()

  #### ---- Configure TensorFlow_BUILD_DIR
  # Order of precidence is 1) CMake variable value, 2) Environmental Variable value
  if(IS_DIRECTORY "${TensorFlow_BUILD_DIR}")
    set(TensorFlow_BUILD_DIR "${TensorFlow_BUILD_DIR}" CACHE PATH "directory containing the file 'libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}'")
  else()
    if(IS_DIRECTORY "$ENV{TENSORFLOW_BUILD_DIR}")
      set(TensorFlow_BUILD_DIR "$ENV{TENSORFLOW_BUILD_DIR}" CACHE PATH "directory containing the file 'libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}'")
    else()
      set(TensorFlow_BUILD_DIR "TensorFlow_BUILD_DIR-NOTFOUND" CACHE PATH "directory containing the file 'libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}'")
    endif()
  endif()

  # Report on status of cmake cache variable for TensorFlow_BUILD_DIR
  if(IS_DIRECTORY ${TensorFlow_BUILD_DIR})
    message(STATUS "TensorFlow_BUILD_DIR is ${TensorFlow_BUILD_DIR}")
  else()
    # NOTE This is not a fatal error for backward compatibility ("custom_op test")
    message(STATUS "No directory at 'TensorFlow_BUILD_DIR:PATH=${TensorFlow_BUILD_DIR}' detected,\n"
      "please specify the path in ENV 'export TENSORFLOW_BUILD_DIR=...'\n or cmake -DTensorFlow_BUILD_DIR:PATH=...\n"
      "to the directory containing the file 'libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}'"
      )
  endif()

  if(IS_DIRECTORY ${TensorFlow_BUILD_DIR})
    file(GLOB_RECURSE TF_LIBRARY_SEARCH_PATHS
      LIST_DIRECTORIES FALSE
      "${TensorFlow_BUILD_DIR}/*libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}"
      )
    list(LENGTH TF_LIBRARY_SEARCH_PATHS TF_LIBRARY_SEARCH_PATHS_LENGTH)
    if( NOT ${TF_LIBRARY_SEARCH_PATHS_LENGTH} EQUAL 1 )
      message(FATAL_ERROR "Incorrect number of items matching 'libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}' in '${TF_LIBRARY_SEARCH_PATHS}'\n"
        "( ${TF_LIBRARY_SEARCH_PATHS_LENGTH} != 1 ).\n"
        "Change 'TensorFlow_BUILD_DIR' to have more specific path."
      )
    endif()
    list(GET TF_LIBRARY_SEARCH_PATHS 0 TF_LIBRARY_SEARCH_ONEPATH)
    get_filename_component(TensorFlow_C_LIBRARY_DIR "${TF_LIBRARY_SEARCH_ONEPATH}" DIRECTORY )

    if( IS_DIRECTORY "${TensorFlow_C_LIBRARY_DIR}")
      find_library(TensorFlow_C_LIBRARY
        NAMES tensorflow_cc
        PATHS "${TensorFlow_C_LIBRARY_DIR}"
        DOC "TensorFlow CC library." )
    endif()
    if( TensorFlow_C_LIBRARY )
      message(STATUS "TensorFlow-CC-LIBRARY is ${TensorFlow_C_LIBRARY}")
    else()
      # NOTE This is not a fatal error for backward compatibility ("custom_op test")
      message(STATUS "No TensorFlow-CC-LIBRARY detected")
    endif()
  endif()

  # tensorflow 1.15 use .so.1 for the library extension in linux. We add the suffix so that it can be found.
  list(APPEND CMAKE_FIND_LIBRARY_SUFFIXES .so.2)
  find_library( TF_DETECTED_LIBRARY
      NAMES tensorflow_framework
      PATHS "${TensorFlow_C_LIBRARY_DIR}" # Prefer the library from the build tree, if TensorFlow_C_LIBRARY is detected.
            "${TF_DETECTED_LIBRARY_PATH}" # use copy of file from the python install tree (This often has a .so.1 extension only for installed version)
      DOC "The tensorflow_framework library path."
  )
  if( TF_DETECTED_LIBRARY )
    message(STATUS "Found: ${TF_DETECTED_LIBRARY}")
  else()
    message(FATAL_ERROR "Required library for tensorflow_framework not found in ${TF_DETECTED_LIBRARY_PATH}!")
  endif()

  # test all given versions
  set(TensorFlow_FOUND FALSE)
  foreach(_TensorFlow_VER ${_TensorFlow_TEST_VERSIONS})
    if("${TF_DETECTED_VERSION_MAJOR}.${TF_DETECTED_VERSION_MINOR}" STREQUAL "${_TensorFlow_VER}")
      # found appropriate version
      set(TensorFlow_VERSION ${TF_DETECTED_VERSION})
      set(TensorFlow_ABI ${TF_DETECTED_ABI})
      set(TensorFlow_INCLUDE_DIR ${TF_DETECTED_INCLUDE_DIR})
      set(TensorFlow_LIBRARY ${TF_DETECTED_LIBRARY})
      set(TensorFlow_FOUND TRUE)
      message(STATUS "Found TensorFlow: (found appropriate version \"${TensorFlow_VERSION}\")")
      message(STATUS "TensorFlow-ABI is ${TensorFlow_ABI}")
      message(STATUS "TensorFlow-INCLUDE_DIR is ${TensorFlow_INCLUDE_DIR}")
      message(STATUS "TensorFlow-LIBRARY is ${TensorFlow_LIBRARY}")

      add_definitions("-DTENSORFLOW_ABI=${TensorFlow_ABI}")
      add_definitions("-DTENSORFLOW_VERSION=${TensorFlow_VERSION}")
      break()
    endif()
  endforeach()

  if(NOT TensorFlow_FOUND)
  message(FATAL_ERROR "Your installed TensorFlow version ${TF_DETECTED_VERSION_MAJOR}.${TF_DETECTED_VERSION_MINOR} is not supported\n"
                      "We tested against ${_TensorFlow_TEST_VERSIONS}")
  endif()

  # test 1.11 version
  if("${TF_DETECTED_VERSION}" VERSION_EQUAL "1.11")
    set(TF_DISABLE_ASSERTS "TRUE")
  endif()

  if("${TF_DETECTED_VERSION}" VERSION_EQUAL "1.12")
    set(TF_DISABLE_ASSERTS "TRUE")
  endif()

  if("${TF_DETECTED_VERSION}" VERSION_EQUAL "1.12.0")
    set(TF_DISABLE_ASSERTS "TRUE")
  endif()

  if("${TF_DETECTED_VERSION}" VERSION_EQUAL "1.13")
    set(TF_DISABLE_ASSERTS "TRUE")
  endif()

  if("${TF_DETECTED_VERSION}" VERSION_EQUAL "1.13.1")
    set(TF_DISABLE_ASSERTS "TRUE")
  endif()

endif() #-- End detection

if(${TF_DISABLE_ASSERTS})
  message(STATUS "[WARNING] The TensorFlow version ${TF_DETECTED_VERSION} has a bug (see \#22766). We disable asserts using -DNDEBUG=True ")
  add_definitions("-DNDEBUG=True")
endif()
macro(TensorFlow_REQUIRE_C_LIBRARY)
  if(NOT EXISTS "${TensorFlow_C_LIBRARY}")
    message(FATAL_ERROR "Project requires libtensorflow_cc${CMAKE_SHARED_LIBRARY_SUFFIX}, please specify the path in ENV 'export TENSORFLOW_BUILD_DIR=...' or cmake -DTensorFlow_BUILD_DIR:PATH=...")
  endif()
endmacro()

macro(TensorFlow_REQUIRE_SOURCE)
  if(NOT IS_DIRECTORY "${TensorFlow_SOURCE_DIR}")
    message(FATAL_ERROR "Project requires TensorFlow source directory, please specify the path in ENV 'export TENSORFLOW_SOURCE_DIR=...' or cmake -DTensorFlow_SOURCE_DIR:PATH=...")
  endif()
endmacro()

macro(add_tensorflow_cpu_operation op_name)
  # Compiles a CPU-only operation without invoking NVCC
  message(STATUS "will build custom TensorFlow operation \"${op_name}\" (CPU only)")

  add_library(${op_name}_op SHARED kernels/${op_name}_op.cc kernels/${op_name}_kernel.cc ops/${op_name}.cc )

  set_target_properties(${op_name}_op PROPERTIES PREFIX "")
  target_link_libraries(${op_name}_op LINK_PUBLIC ${TensorFlow_LIBRARY})
endmacro()


macro(add_tensorflow_gpu_operation op_name)
# Compiles a CPU + GPU operation with invoking NVCC
  message(STATUS "will build custom TensorFlow operation \"${op_name}\" (CPU+GPU)")

  set(kernel_file "")
  if(EXISTS "kernels/${op_name}_kernel.cu")
     message(WARNING "you should rename your file ${op_name}_kernel.cu to ${op_name}_kernel_gpu.cu.cc")
     set(kernel_file kernels/${op_name}_kernel.cu)
  else()
    set_source_files_properties(kernels/${op_name}_kernel_gpu.cu.cc PROPERTIES CUDA_SOURCE_PROPERTY_FORMAT OBJ)
     set(kernel_file kernels/${op_name}_kernel_gpu.cu.cc)
  endif()

  cuda_add_library(${op_name}_op_cu SHARED ${kernel_file})
  set_target_properties(${op_name}_op_cu PROPERTIES PREFIX "")

  add_library(${op_name}_op SHARED kernels/${op_name}_op.cc kernels/${op_name}_kernel.cc ops/${op_name}.cc )

  set_target_properties(${op_name}_op PROPERTIES PREFIX "")
  set_target_properties(${op_name}_op PROPERTIES COMPILE_FLAGS "-DGOOGLE_CUDA")
  target_link_libraries(${op_name}_op LINK_PUBLIC ${op_name}_op_cu ${TensorFlow_LIBRARY})
endmacro()

# simplify TensorFlow dependencies
add_library(TensorFlow_DEP INTERFACE)
target_include_directories(TensorFlow_DEP SYSTEM INTERFACE ${TensorFlow_SOURCE_DIR})
target_include_directories(TensorFlow_DEP SYSTEM INTERFACE ${TensorFlow_INCLUDE_DIR})
target_link_libraries(TensorFlow_DEP INTERFACE -Wl,--allow-multiple-definition -Wl,--whole-archive ${TensorFlow_C_LIBRARY} -Wl,--no-whole-archive)
target_link_libraries(TensorFlow_DEP INTERFACE -Wl,--allow-multiple-definition -Wl,--whole-archive ${TensorFlow_LIBRARY} -Wl,--no-whole-archive)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  TENSORFLOW
  FOUND_VAR TENSORFLOW_FOUND
  REQUIRED_VARS
    TensorFlow_LIBRARY
    TensorFlow_INCLUDE_DIR
  VERSION_VAR
    TensorFlow_VERSION
  )

mark_as_advanced(TF_INFORMATION_STRING TF_DETECTED_VERSION TF_DETECTED_VERSION_MAJOR TF_DETECTED_VERSION_MINOR TF_DETECTED_VERSION TF_DETECTED_ABI
                 TF_DETECTED_INCLUDE_DIR TF_DETECTED_LIBRARY TF_DISABLE_ASSERTS
                 TensorFlow_C_LIBRARY TensorFlow_LIBRARY TensorFlow_SOURCE_DIR TensorFlow_INCLUDE_DIR TensorFlow_ABI)

set(TensorFlow_INCLUDE_DIR "${TensorFlow_INCLUDE_DIR}" CACHE PATH "The path to tensorflow header files")
set(TensorFlow_VERSION "${TensorFlow_VERSION}" CACHE INTERNAL "The Tensorflow version")
set(TensorFlow_ABI "${TensorFlow_ABI}" CACHE STRING "The ABI version used by TensorFlow")
set(TensorFlow_LIBRARY "${TensorFlow_LIBRARY}" CACHE FILEPATH "The C++ library of TensorFlow")
set(TensorFlow_C_LIBRARY "${TensorFlow_C_LIBRARY}" CACHE STRING "The C library of TensorFlow")
set(TensorFlow_FOUND "${TensorFlow_FOUND}" CACHE BOOL "A flag stating if TensorFlow has been found")
set(TF_DISABLE_ASSERTS "${TF_DISABLE_ASSERTS}" CACHE BOOL "A flag to enable workarounds")
