# Copyright (C) 1995-2019, Rene Brun and Fons Rademakers.
# All rights reserved.
#
# For the licensing terms see $ROOTSYS/LICENSE.
# For the list of contributors see $ROOTSYS/README/CREDITS.

############################################################################
# CMakeLists.txt file for building ROOT core/rootcling_stage1 package
############################################################################

if(WIN32)
  set_source_files_properties(src/rootcling_stage1.cxx PROPERTIES COMPILE_FLAGS "-D_WIN32 -DNOMINMAX")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /ignore:4217,4286")
else()
  if(CXX_HAS_fno_rtti)
    set_source_files_properties(src/rootcling_stage1.cxx PROPERTIES COMPILE_FLAGS "-fno-rtti")
  endif()
endif()

if(builtin_clang)
  set(CLING_LIBRARIES "clingMetaProcessor")
else()
  set(clang_libraries)
  if(MSVC)
    ROOT_GET_CLANG_LIBRARIES(clang_libraries)
  endif()
  list(APPEND CLING_LIBRARIES ${CLING_DEPEND_LIBS} "${clang_libraries}")
  link_directories("${LLVM_LIBRARY_DIR}")
endif()

ROOT_EXECUTABLE(rootcling_stage1 src/rootcling_stage1.cxx
                              $<TARGET_OBJECTS:Clib>
                              $<TARGET_OBJECTS:ClingUtils>
                              $<TARGET_OBJECTS:Dictgen>
                              $<TARGET_OBJECTS:Foundation_Stage1>
                              LIBRARIES ${CLING_LIBRARIES} ${CMAKE_DL_LIBS} ${CMAKE_THREAD_LIBS_INIT} ${ROOT_ATOMIC_LIBS}
                              NOINSTALL)

function(print_recursive_dependencies target)
    print_recursive_dependencies_impl("${target}" "" "")
endfunction()

function(print_recursive_dependencies_impl target indent visited)
    # Check if target exists
    if(NOT TARGET ${target})
        message(STATUS "${indent}${target} (not a CMake target)")
        return()
    endif()
    
    # Check if we've already visited this target (avoid infinite loops)
    list(FIND visited ${target} already_visited)
    if(NOT already_visited EQUAL -1)
        message(STATUS "${indent}${target} (already visited)")
        return()
    endif()
    
    # Add current target to visited list
    list(APPEND visited ${target})
    
    # Get LINK_LIBRARIES property
    get_target_property(link_libs ${target} LINK_LIBRARIES)
    
    # Get INTERFACE_LINK_LIBRARIES property
    get_target_property(interface_libs ${target} INTERFACE_LINK_LIBRARIES)
    
    # Print LINK_LIBRARIES
    if(link_libs)
        message(STATUS "${indent}${target} [LINK_LIBRARIES]:")
        foreach(lib ${link_libs})
            message(STATUS "${indent}  -> ${lib}")
            print_recursive_dependencies_impl("${lib}" "${indent}    " "${visited}")
        endforeach()
    else()
        message(STATUS "${indent}${target} [LINK_LIBRARIES]: (none)")
    endif()
    
    # Print INTERFACE_LINK_LIBRARIES
    if(interface_libs)
        message(STATUS "${indent}${target} [INTERFACE_LINK_LIBRARIES]:")
        foreach(lib ${interface_libs})
            message(STATUS "${indent}  -> ${lib}")
            print_recursive_dependencies_impl("${lib}" "${indent}    " "${visited}")
        endforeach()
    else()
        message(STATUS "${indent}${target} [INTERFACE_LINK_LIBRARIES]: (none)")
    endif()
endfunction()

function(find_llvm_dependency target indent)
    if(NOT TARGET ${target})
        return()
    endif()
    
    get_target_property(link_libs ${target} LINK_LIBRARIES)
    get_target_property(interface_libs ${target} INTERFACE_LINK_LIBRARIES)
    
    # Check LINK_LIBRARIES
    if(link_libs)
        foreach(lib ${link_libs})
            if("${lib}" STREQUAL "LLVM")
                message(STATUS "${indent}>>> FOUND IT: ${target} links against LLVM target")
            endif()
            if(TARGET ${lib})
                find_llvm_dependency(${lib} "${indent}  ")
            endif()
        endforeach()
    endif()
    
    # Check INTERFACE_LINK_LIBRARIES  
    if(interface_libs)
        foreach(lib ${interface_libs})
            if("${lib}" STREQUAL "LLVM")
                message(STATUS "${indent}>>> FOUND IT: ${target} has LLVM in INTERFACE_LINK_LIBRARIES")
            endif()
            if(TARGET ${lib})
                find_llvm_dependency(${lib} "${indent}  ")
            endif()
        endforeach()
    endif()
endfunction()

message(STATUS "Printing recursive dependencies for rootcling_stage1...")
print_recursive_dependencies(rootcling_stage1)

message(STATUS "Checking if rootcling_stage1 links against LLVM target...")
find_llvm_dependency(rootcling_stage1 "")

target_include_directories(rootcling_stage1 PRIVATE
   ${CMAKE_SOURCE_DIR}/core/foundation/inc    # for RConfig.hxx
   ${CMAKE_SOURCE_DIR}/core/dictgen/res # for rootcling_impl.h
   ${CMAKE_BINARY_DIR}/ginclude         # for RConfigure.h
)
if (CMAKE_SYSTEM_NAME MATCHES FreeBSD)
  target_link_libraries(rootcling_stage1 util procstat)
endif()

if(NOT (WIN32 AND CMAKE_GENERATOR MATCHES Ninja))
  set_target_properties(rootcling_stage1 PROPERTIES RUNTIME_OUTPUT_DIRECTORY src)
endif()
add_dependencies(rootcling_stage1 ClingUtils)
