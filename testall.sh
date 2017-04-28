#!/bin/bash

# Assume you've already installed Linuxbrew. 
# This software has the following dependencies:
# AVX hardware support by Intel, GCC 5.3 (Linux) or Apple LLVM version 7.0.2 (Mac), clang-format, cmake 2.8 or higher (C++), jemalloc (C++), tbb (C++, Intel), cython (python), sbt

# Step 0. Install if they're not there. 
declare -a dependencies=("gcc" "clang-format" "cmake" "jemalloc" "tbb" "sbt")
for app in "${dependencies[@]}" ; do 
	brew install $app
done
pip install Cython

# Change current directory to wu636_cs525_project/
# Step 1. Generate program code
for i in 1 2 4 8 16; do
	sbt "test:run c triangle_counting_${i}thread"
	FILE=src/out/triangle_counting_${i}thread.c
	if [ -f $FILE ]; then
		mv $FILE src/out/storage_engine/apps/triangle_counting_${i}thread.cpp
	fi
done

# Step 2. Compile our program with storage_engine library using CMake, and execute it! 
cd src/out/storage_engine/
for i in 1 2 4 8 16; do
	if [ -d build ]; then
		rm -rf build
	fi
	mkdir build
	cd build
	cmake -DNUM_THREADS=$i -DCMAKE_C_COMPILER=gcc-5 -DCMAKE_CXX_COMPILER=g++-5 .. >/dev/null
	make
	./bin/triangle_counting_${i}thread
	cd -
done
cd ../../../


