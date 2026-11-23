# SPDX-License-Identifier: Apache-2.0
cmake_minimum_required(VERSION 3.16)
project(ub_test_http LANGUAGES CXX)

set(BRPC_ROOT "" CACHE PATH "Installed urma-dev-bk prefix (include and lib or lib64)")
set(URMA_ROOT "/usr" CACHE PATH "UMDK installation prefix for the real provider")
option(WITH_URMA "Compile real URMA support against the selected brpc installation" ON)
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT EXISTS "${BRPC_ROOT}/include/brpc/socket_mode.h" OR NOT BRPC_ROOT)
    message(FATAL_ERROR "Set BRPC_ROOT to an installed urma-dev-bk prefix with brpc/socket_mode.h")
endif()
# Use the same prefix on every configure, including when switching installations
# in an existing build directory. Do not fall through to a system brpc.
unset(BRPC_LIBRARY CACHE)
find_file(BRPC_LIBRARY NAMES "${CMAKE_SHARED_LIBRARY_PREFIX}brpc${CMAKE_SHARED_LIBRARY_SUFFIX}"
          PATHS "${BRPC_ROOT}/lib" "${BRPC_ROOT}/lib64" NO_DEFAULT_PATH)
if(NOT BRPC_LIBRARY)
    message(FATAL_ERROR "Shared libbrpc missing under ${BRPC_ROOT}/{lib,lib64}")
endif()
message(STATUS "Using brpc headers at ${BRPC_ROOT}/include and ${BRPC_LIBRARY}")

find_package(Threads REQUIRED)
find_package(Protobuf CONFIG QUIET)
if(NOT TARGET protobuf::libprotobuf)
    find_package(Protobuf MODULE REQUIRED)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PROTOBUF_PC QUIET IMPORTED_TARGET protobuf)
    endif()
endif()
find_path(GFLAGS_INCLUDE_DIR gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags)
if(NOT GFLAGS_INCLUDE_DIR OR NOT GFLAGS_LIBRARY)
    message(FATAL_ERROR "gflags development files required")
endif()
find_package(OpenSSL REQUIRED)

set(generated)
foreach(schema test ub_test_http)
    set(input "${CMAKE_CURRENT_SOURCE_DIR}/${schema}.proto")
    set(cc "${CMAKE_CURRENT_BINARY_DIR}/${schema}.pb.cc")
    set(header "${CMAKE_CURRENT_BINARY_DIR}/${schema}.pb.h")
    add_custom_command(OUTPUT "${cc}" "${header}"
        COMMAND protobuf::protoc "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}"
                "--cpp_out=${CMAKE_CURRENT_BINARY_DIR}" "${input}"
        DEPENDS "${input}" protobuf::protoc VERBATIM)
    list(APPEND generated "${cc}" "${header}")
endforeach()

add_executable(ub_test_http ub_test_http.cpp ${generated})
target_include_directories(ub_test_http PRIVATE "${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}"
    "${BRPC_ROOT}/include" "${GFLAGS_INCLUDE_DIR}")
target_compile_definitions(ub_test_http PRIVATE BRPC_WITH_URMA=$<BOOL:${WITH_URMA}>)
target_link_libraries(ub_test_http PRIVATE "${BRPC_LIBRARY}" "${GFLAGS_LIBRARY}"
    protobuf::libprotobuf OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
if(TARGET PkgConfig::PROTOBUF_PC)
    target_link_libraries(ub_test_http PRIVATE PkgConfig::PROTOBUF_PC)
endif()
get_filename_component(brpc_libdir "${BRPC_LIBRARY}" DIRECTORY)
set(runtime_dirs "${brpc_libdir}")
if(WITH_URMA)
    if(URMA_ROOT)
        set(sdk_paths PATHS "${URMA_ROOT}" NO_DEFAULT_PATH)
    else()
        set(sdk_paths HINTS ENV URMA_ROOT)
    endif()
    unset(URMA_INCLUDE_DIR CACHE)
    unset(URMA_LIBRARY CACHE)
    find_path(URMA_INCLUDE_DIR urma_api.h ${sdk_paths}
        PATH_SUFFIXES include/ub/umdk/urma include/umdk/urma include/urma include
                      ub/umdk/urma umdk/urma urma)
    find_library(URMA_LIBRARY NAMES urma ${sdk_paths} PATH_SUFFIXES lib64 lib)
    if(NOT URMA_INCLUDE_DIR OR NOT URMA_LIBRARY)
        message(FATAL_ERROR "Real URMA support requires UMDK headers and liburma. Set URMA_ROOT.")
    endif()
    target_include_directories(ub_test_http PRIVATE "${URMA_INCLUDE_DIR}")
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        # The probe calls brpc's URMA wrappers. Retain the SDK as a direct
        # dependency so the executable's RUNPATH also resolves liburma.
        target_link_libraries(ub_test_http PRIVATE "-Wl,--push-state,--no-as-needed"
            "${URMA_LIBRARY}" "-Wl,--pop-state")
    else()
        target_link_libraries(ub_test_http PRIVATE "${URMA_LIBRARY}")
    endif()
    get_filename_component(urma_libdir "${URMA_LIBRARY}" DIRECTORY)
    list(APPEND runtime_dirs "${urma_libdir}")
endif()
set_target_properties(ub_test_http PROPERTIES INSTALL_RPATH "${runtime_dirs}"
    BUILD_WITH_INSTALL_RPATH ON)
install(TARGETS ub_test_http RUNTIME DESTINATION bin)
