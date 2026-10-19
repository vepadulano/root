############################################################################
# CMakeLists.txt file for building the ONNXToCpp library.
#
# ONNXToCpp converts ONNX model files to standalone C++ code that depends
# only on the C++ standard library. The library itself has no dependencies
# besides the C++ standard library (C++17 is enough).
#
# This file is meant to work both when the directory is part of the ROOT
# source tree, and when it is built on its own. Inside ROOT, the library is
# private: its headers are not installed and no dictionary is generated.
############################################################################

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(ONNXTOCPP_STANDALONE TRUE)
  cmake_minimum_required(VERSION 3.16)
  project(ONNXToCpp LANGUAGES CXX)
  # The library only needs C++17, but it follows the standard chosen by the user
  if(NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
  endif()
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  include(GNUInstallDirs)
  option(ONNXTOCPP_BUILD_TESTS "Build the tests of ONNXToCpp" OFF)
else()
  set(ONNXTOCPP_STANDALONE FALSE)
  # The library is only used by RooFit
  if(NOT roofit)
    return()
  endif()
  set(ONNXTOCPP_BUILD_TESTS ${testing})
endif()

# The library target keeps the name ROOT for now, if this is moved outside of the repository it can be dropped.
add_library(ROOTInternalONNXToCpp SHARED
  src/RModel.cxx
  src/common.cxx
  src/common_helpers.cxx
  src/RModelParser_ONNX.cxx
  src/ParseBasicUnary.cxx
  src/ParseBasicBinary.cxx
  src/ParseBasicIs.cxx
  src/ParseBatchNormalization.cxx
  src/ParseCast.cxx
  src/ParseConcat.cxx
  src/ParseConv.cxx
  src/ParseConvTranspose.cxx
  src/ParseGemm.cxx
  src/ParseGRU.cxx
  src/ParseIdentity.cxx
  src/ParseLeakyRelu.cxx
  src/ParseLSTM.cxx
  src/ParseBasicNary.cxx
  src/ParsePool.cxx
  src/ParseReduce.cxx
  src/ParseRelu.cxx
  src/ParseReshape.cxx
  src/ParseRNN.cxx
  src/ParseSelu.cxx
  src/ParseShape.cxx
  src/ParseSigmoid.cxx
  src/ParseSwish.cxx
  src/ParseSlice.cxx
  src/ParseSoftmax.cxx
  src/ParseTanh.cxx
  src/ParseTranspose.cxx
  src/ParseErf.cxx
  src/ParseRange.cxx
  src/ParseLayerNormalization.cxx
  src/ParseInstanceNormalization.cxx
  src/ParseExpand.cxx
  src/ParseGather.cxx
  src/ParseGatherND.cxx
  src/ParseElu.cxx
  src/ParseFuseConvAdd.cxx
  src/ParseFuseConvTransposeAdd.cxx
  src/ParseFuseGemmRelu.cxx
  src/ParseFuseBatchnormRelu.cxx
  src/ParseFuseMatMulAdd.cxx
  src/ParseMatMul.cxx
  src/ParseComparision.cxx
  src/ParseEyeLike.cxx
  src/ParseConstant.cxx
  src/ParseTopK.cxx
  src/ParseTile.cxx
  src/ParseSplit.cxx
  src/ParseIf.cxx
  src/ParsePad.cxx
  src/ParseWhere.cxx
  src/ParseEinsum.cxx
  src/ParseRandom.cxx
  src/ParseScatterElements.cxx
  src/ParseScatterND.cxx
  src/ParseNonZero.cxx
  src/ParseNot.cxx
  src/ParseClip.cxx
  src/ParseGelu.cxx
  src/ParseHardSigmoid.cxx
  src/ParseHardSwish.cxx
)

target_include_directories(ROOTInternalONNXToCpp
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/inc>
  PRIVATE
    # onnx.hxx, the self-contained ONNX protobuf reader, is private
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)


if(ONNXTOCPP_STANDALONE)
  install(TARGETS ROOTInternalONNXToCpp
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(DIRECTORY inc/ONNXToCpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
else()
  # Inside ROOT the library is an implementation detail of ROOT: avoid
  # clashes with the public ROOT libraries, and install only the library (not
  # its headers). The headers are only included by code using them and they
  # are not needed at runtime. It still needs to be part of the ROOT export set,
  # since ROOT libraries link to it.
  ROOT_APPEND_LIBDIR_TO_INSTALL_RPATH(ROOTInternalONNXToCpp ${CMAKE_INSTALL_LIBDIR})
  set_property(GLOBAL APPEND PROPERTY ROOT_EXPORTED_TARGETS ROOTInternalONNXToCpp)
  install(TARGETS ROOTInternalONNXToCpp EXPORT ${CMAKE_PROJECT_NAME}Exports
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} COMPONENT libraries
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libraries
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} COMPONENT libraries)

  # The tests of the differentiation of the generated code with Clad need the
  # interpreter and ROOT's testing infrastructure
  if(testing AND clad)
    ROOT_ADD_TEST_SUBDIRECTORY(test_clad)
  endif()
endif()

if(ONNXTOCPP_BUILD_TESTS)
  if(ONNXTOCPP_STANDALONE)
    enable_testing()
  endif()
  add_subdirectory(test)
endif()
