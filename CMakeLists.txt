cmake_minimum_required(VERSION 3.15)
project(c_kafka_engine LANGUAGES C CXX)

# Force modern C and C++ language standards across all compilers
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -----------------------------------------------------------------------------
# OS-Specific Pre-configurations (macOS Homebrew path injection)
# -----------------------------------------------------------------------------
if(APPLE)
    # Automatically scan default Apple Silicon Homebrew paths if they exist
    if(EXISTS "/opt/homebrew/include")
        list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew")
        include_directories("/opt/homebrew/include")
        link_directories("/opt/homebrew/lib")
    endif()
endif()

# -----------------------------------------------------------------------------
# Dependency Tracking Layer
# -----------------------------------------------------------------------------
# Find Threading support natively (pthread on POSIX, native windows threads on MSVC)
find_package(Threads REQUIRED)

# Find AWS SDK C++ Components
# Note: AWS SDK installs its own CMake target modules automatically
find_package(AWSSDK REQUIRED COMPONENTS core dynamodb)

# Find librdkafka (Fallback manually if find_package module isn't globally available)
find_package(rdkafka CONFIG QUIET)
if(NOT rdkafka_FOUND)
    # Manual fallback for traditional UNIX setups where config file isn't present
    find_path(RDKAFKA_INCLUDE_DIR NAMES librdkafka/rdkafka.h)
    find_library(RDKAFKA_LIBRARY NAMES rdkafka)
    
    if(NOT RDKAFKA_INCLUDE_DIR OR NOT RDKAFKA_LIBRARY)
        message(FATAL_ERROR "Could not locate librdkafka headers or libraries!")
    endif()
else()
    # If found via modern config target, extract properties directly
    set(RDKAFKA_LIBRARY rdkafka::rdkafka)
endif()

# -----------------------------------------------------------------------------
# Target Executable Compilation Geometry
# -----------------------------------------------------------------------------
# Define the core binary artifact executable layout
add_executable(c_kafka_engine 
    main.c 
    aws_dynamo_wrapper.cpp 
    aws_dynamo_wrapper.h
)

# Bind include boundaries so files find each other
target_include_directories(c_kafka_engine PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
if(RDKAFKA_INCLUDE_DIR)
    target_include_directories(c_kafka_engine PRIVATE ${RDKAFKA_INCLUDE_DIR})
endif()

# -----------------------------------------------------------------------------
# Linker Configurations & Windows Layer Extensions
# -----------------------------------------------------------------------------
# Bind third-party shared targets cleanly to your master engine
target_link_libraries(c_kafka_engine PRIVATE
    Threads::Threads
    ${AWSSDK_LINK_LIBRARIES}
    ${RDKAFKA_LIBRARY}
)

if(WIN32)
    # Windows platform requires linking against the native WinSock2 system network stack
    target_link_libraries(c_kafka_engine PRIVATE ws2_32)
    
    # Optional target adjustments for Windows compilation (disables MSVC security alerts)
    target_compile_definitions(c_kafka_engine PRIVATE _CRT_SECURE_NO_WARNINGS)
endif()
