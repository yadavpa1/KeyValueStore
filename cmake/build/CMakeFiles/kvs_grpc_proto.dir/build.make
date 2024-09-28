# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/catchashu10/KeyValueStore

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/catchashu10/KeyValueStore/cmake/build

# Include any dependencies generated for this target.
include CMakeFiles/kvs_grpc_proto.dir/depend.make
# Include any dependencies generated by the compiler for this target.
include CMakeFiles/kvs_grpc_proto.dir/compiler_depend.make

# Include the progress variables for this target.
include CMakeFiles/kvs_grpc_proto.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/kvs_grpc_proto.dir/flags.make

keyvaluestore.pb.cc: ../../keyvaluestore.proto
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --blue --bold --progress-dir=/home/catchashu10/KeyValueStore/cmake/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Generating keyvaluestore.pb.cc, keyvaluestore.pb.h, keyvaluestore.grpc.pb.cc, keyvaluestore.grpc.pb.h"
	/home/catchashu10/.local/bin/protoc-27.2.0 --grpc_out /home/catchashu10/KeyValueStore/cmake/build --cpp_out /home/catchashu10/KeyValueStore/cmake/build -I /home/catchashu10/KeyValueStore --plugin=protoc-gen-grpc="/home/catchashu10/.local/bin/grpc_cpp_plugin" /home/catchashu10/KeyValueStore/keyvaluestore.proto

keyvaluestore.pb.h: keyvaluestore.pb.cc
	@$(CMAKE_COMMAND) -E touch_nocreate keyvaluestore.pb.h

keyvaluestore.grpc.pb.cc: keyvaluestore.pb.cc
	@$(CMAKE_COMMAND) -E touch_nocreate keyvaluestore.grpc.pb.cc

keyvaluestore.grpc.pb.h: keyvaluestore.pb.cc
	@$(CMAKE_COMMAND) -E touch_nocreate keyvaluestore.grpc.pb.h

CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o: CMakeFiles/kvs_grpc_proto.dir/flags.make
CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o: keyvaluestore.grpc.pb.cc
CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o: CMakeFiles/kvs_grpc_proto.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/catchashu10/KeyValueStore/cmake/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o -MF CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o.d -o CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o -c /home/catchashu10/KeyValueStore/cmake/build/keyvaluestore.grpc.pb.cc

CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/catchashu10/KeyValueStore/cmake/build/keyvaluestore.grpc.pb.cc > CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.i

CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/catchashu10/KeyValueStore/cmake/build/keyvaluestore.grpc.pb.cc -o CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.s

CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o: CMakeFiles/kvs_grpc_proto.dir/flags.make
CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o: keyvaluestore.pb.cc
CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o: CMakeFiles/kvs_grpc_proto.dir/compiler_depend.ts
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/catchashu10/KeyValueStore/cmake/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -MD -MT CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o -MF CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o.d -o CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o -c /home/catchashu10/KeyValueStore/cmake/build/keyvaluestore.pb.cc

CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/catchashu10/KeyValueStore/cmake/build/keyvaluestore.pb.cc > CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.i

CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/catchashu10/KeyValueStore/cmake/build/keyvaluestore.pb.cc -o CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.s

# Object files for target kvs_grpc_proto
kvs_grpc_proto_OBJECTS = \
"CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o" \
"CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o"

# External object files for target kvs_grpc_proto
kvs_grpc_proto_EXTERNAL_OBJECTS =

libkvs_grpc_proto.a: CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.grpc.pb.cc.o
libkvs_grpc_proto.a: CMakeFiles/kvs_grpc_proto.dir/keyvaluestore.pb.cc.o
libkvs_grpc_proto.a: CMakeFiles/kvs_grpc_proto.dir/build.make
libkvs_grpc_proto.a: CMakeFiles/kvs_grpc_proto.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/catchashu10/KeyValueStore/cmake/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Linking CXX static library libkvs_grpc_proto.a"
	$(CMAKE_COMMAND) -P CMakeFiles/kvs_grpc_proto.dir/cmake_clean_target.cmake
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/kvs_grpc_proto.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/kvs_grpc_proto.dir/build: libkvs_grpc_proto.a
.PHONY : CMakeFiles/kvs_grpc_proto.dir/build

CMakeFiles/kvs_grpc_proto.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/kvs_grpc_proto.dir/cmake_clean.cmake
.PHONY : CMakeFiles/kvs_grpc_proto.dir/clean

CMakeFiles/kvs_grpc_proto.dir/depend: keyvaluestore.grpc.pb.cc
CMakeFiles/kvs_grpc_proto.dir/depend: keyvaluestore.grpc.pb.h
CMakeFiles/kvs_grpc_proto.dir/depend: keyvaluestore.pb.cc
CMakeFiles/kvs_grpc_proto.dir/depend: keyvaluestore.pb.h
	cd /home/catchashu10/KeyValueStore/cmake/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/catchashu10/KeyValueStore /home/catchashu10/KeyValueStore /home/catchashu10/KeyValueStore/cmake/build /home/catchashu10/KeyValueStore/cmake/build /home/catchashu10/KeyValueStore/cmake/build/CMakeFiles/kvs_grpc_proto.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/kvs_grpc_proto.dir/depend

