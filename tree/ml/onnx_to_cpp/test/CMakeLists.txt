############################################################################
# CMakeLists.txt file for building the ONNXToCpp tests, which run with ctest.
# @author Federico Sossai, Sanjiban Sengupta
#
# The generated models are compiled by the tests with the C++ compiler used to
# build them, see driver_runner.h.
############################################################################

if(NOT TARGET GTest::gtest OR NOT TARGET GTest::gtest_main)
  find_package(GTest REQUIRED)
endif()
include(GoogleTest)

find_package(Python3 COMPONENTS Interpreter)

# BLAS is needed by the models that use Gemm (optional)
find_package(BLAS)

# nlohmann_json is used by the safetensors test to validate the structure of the
# generated weight file.
if(NOT TARGET nlohmann_json::nlohmann_json)
  find_package(nlohmann_json QUIET)
endif()

# Look for the needed Python modules
function(onnxtocpp_find_python_module module)
  string(TOUPPER ${module} MODULE)
  set(ONNXTOCPP_${MODULE}_FOUND FALSE PARENT_SCOPE)
  if(Python3_Interpreter_FOUND)
    execute_process(
      COMMAND ${Python3_EXECUTABLE} -c "import ${module}; print(${module}.__version__)"
      RESULT_VARIABLE _status
      OUTPUT_VARIABLE _version
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET)
    if(_status EQUAL 0)
      set(ONNXTOCPP_${MODULE}_FOUND TRUE PARENT_SCOPE)
      set(ONNXTOCPP_${MODULE}_VERSION ${_version} PARENT_SCOPE)
    endif()
  endif()
endfunction()

onnxtocpp_find_python_module(torch)

# onnx 1.19.0 has a bug that makes this version unusable:
# https://github.com/onnx/onnx/issues/7249
# 1.19.1 instead could be used
# In that case, we have to disable the "TestONNXToCppModels" test,
# which imports onnx indirectly via torch.onnx
onnxtocpp_find_python_module(onnx)
if(ONNXTOCPP_ONNX_FOUND AND DEFINED ONNXTOCPP_ONNX_VERSION)
  if(ONNXTOCPP_ONNX_VERSION VERSION_EQUAL "1.19.0")
    message(WARNING "Found broken onnx version ${ONNXTOCPP_ONNX_VERSION} (see https://github.com/onnx/onnx/issues/7249). Some ONNXToCpp tests will be disabled.")
    set(broken_onnx TRUE)
  endif()
endif()

# Configuration of the drivers that run the generated models
set(ONNXTOCPP_TEST_CXX_FLAGS "-std=c++${CMAKE_CXX_STANDARD}")
set(ONNXTOCPP_TEST_LINK_FLAGS "")
if(BLAS_FOUND)
  set(_blas_flags ${BLAS_LINKER_FLAGS})
  foreach(_lib ${BLAS_LIBRARIES})
    # A macOS framework (e.g. Accelerate) is linked with -framework
    if(_lib MATCHES "^(.*)/([^/]+)\\.framework$")
      list(APPEND _blas_flags -framework ${CMAKE_MATCH_2})
    else()
      list(APPEND _blas_flags ${_lib})
    endif()
  endforeach()
  string(JOIN " " ONNXTOCPP_TEST_LINK_FLAGS ${_blas_flags})
endif()
set(ONNXTOCPP_TEST_CLAD_FLAGS "")
configure_file(test_config.h.in test_config.h @ONLY)

# Adds a test executable with Google Test. Each test case is registered as a test
# for ctest, so that they can run in parallel.
function(onnxtocpp_add_gtest name source)
  cmake_parse_arguments(ARG "" "" "LIBRARIES;FIXTURES_REQUIRED" ${ARGN})
  add_executable(${name} ${source})
  target_link_libraries(${name} PRIVATE ROOTInternalONNXToCpp GTest::gtest GTest::gtest_main ${ARG_LIBRARIES})
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR})
  if(ARG_FIXTURES_REQUIRED)
    gtest_discover_tests(${name} TEST_PREFIX "${name}." PROPERTIES FIXTURES_REQUIRED ${ARG_FIXTURES_REQUIRED})
  else()
    gtest_discover_tests(${name} TEST_PREFIX "${name}.")
  endif()
endfunction()

# Adding a model to generate_input_models.py changes the model list below, so
# CMake has to re-run when the script changes. Without this, an incremental
# build keeps the stale list and the new model is silently never emitted.
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS
             ${CMAKE_CURRENT_SOURCE_DIR}/generate_input_models.py)

# The ONNX input models are not checked into the repository: they are created
# by generate_input_models.py, which runs as the ONNXToCppGenerateModels_ONNX
# test that the other ONNX tests depend on. Set ONNX_MODELS_DIR to use
# pre-existing models from a custom directory instead.
if(NOT ONNX_MODELS_DIR)
  if(ONNXTOCPP_ONNX_FOUND AND NOT broken_onnx)
    set(ONNX_MODELS_DIR ${CMAKE_CURRENT_BINARY_DIR}/input_models)
    set(generate_onnx_models TRUE)
    # Get the model names from the generator script (--list does not require
    # the onnx module, only a Python interpreter).
    execute_process(
      COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/generate_input_models.py --list
      OUTPUT_VARIABLE _onnx_model_names
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE _onnx_list_status)
    if(NOT _onnx_list_status EQUAL 0)
      message(FATAL_ERROR "Failed to list the ONNXToCpp ONNX test models with generate_input_models.py")
    endif()
    string(REPLACE "\n" ";" ONNX_MODEL_NAMES "${_onnx_model_names}")
  else()
    message(WARNING "The onnx Python module is not usable: the ONNX input models for the ONNXToCpp tests cannot be generated and the corresponding tests are disabled.")
  endif()
else()
  get_filename_component(ONNX_MODELS_DIR ${ONNX_MODELS_DIR} ABSOLUTE)
  file(GLOB ONNX_FILES "${ONNX_MODELS_DIR}/*.onnx")
  set(ONNX_MODEL_NAMES "")
  foreach(onnx_file ${ONNX_FILES})
    get_filename_component(fname ${onnx_file} NAME_WE)
    list(APPEND ONNX_MODEL_NAMES ${fname})
  endforeach()
endif()

if(ONNX_MODELS_DIR)
  # Creating the appropriate code to parse all ONNX model files. It is much
  # faster to combine all parsing in a single executable which will avoid
  # initialization time
  set(CAPTURE_STR "EmitModel( \"@1\", \"@2\");")
  set(ALL_CAPTURES "")
  foreach(fname ${ONNX_MODEL_NAMES})
    string(REPLACE "@1" ${ONNX_MODELS_DIR}/${fname}.onnx cap ${CAPTURE_STR})
    string(REPLACE "@2" ${fname} cap ${cap})
    list(APPEND ALL_CAPTURES ${cap})
  endforeach()
  string(REPLACE ";" ";\n" EMIT_CAPTURES "${ALL_CAPTURES}")
  configure_file(ONNXToCppEmitFromONNX.cxx.in ONNXToCppEmitFromONNX_all.cxx @ONLY)

  add_executable(ONNXToCppEmitFromONNX ${CMAKE_CURRENT_BINARY_DIR}/ONNXToCppEmitFromONNX_all.cxx)
  target_link_libraries(ONNXToCppEmitFromONNX PRIVATE ROOTInternalONNXToCpp)

  set(compile_models_fixtures "")
  if(generate_onnx_models)
    add_test(NAME ONNXToCppGenerateModels_ONNX
             COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/generate_input_models.py --outdir ${ONNX_MODELS_DIR})
    set_tests_properties(ONNXToCppGenerateModels_ONNX PROPERTIES FIXTURES_SETUP onnxtocpp-generate-models-onnx)
    list(APPEND compile_models_fixtures onnxtocpp-generate-models-onnx)
  endif()

  add_test(NAME ONNXToCppCompileModels_ONNX COMMAND ONNXToCppEmitFromONNX)
  set_tests_properties(ONNXToCppCompileModels_ONNX PROPERTIES
                       FIXTURES_SETUP onnxtocpp-compile-models-onnx)
  if(compile_models_fixtures)
    set_tests_properties(ONNXToCppCompileModels_ONNX PROPERTIES FIXTURES_REQUIRED "${compile_models_fixtures}")
  endif()

  # Tests of the alias tensors. They assert on the RModel itself rather than on
  # the code it generates, so they need the ONNX models but none of the headers
  # emitted from them.
  set(alias_test_fixtures "")
  if(generate_onnx_models)
    set(alias_test_fixtures onnxtocpp-generate-models-onnx)
  endif()
  onnxtocpp_add_gtest(TestONNXToCppAlias TestONNXToCppAlias.cxx FIXTURES_REQUIRED ${alias_test_fixtures})
  target_compile_definitions(TestONNXToCppAlias PRIVATE ONNXTOCPP_ONNX_MODELS_DIR="${ONNX_MODELS_DIR}")

  # The models are compiled by the tests, and they need BLAS to do so
  if(BLAS_FOUND)
    onnxtocpp_add_gtest(TestONNXToCppCustomModelsFromONNX TestONNXToCppCustomModelsFromONNX.cxx
                        FIXTURES_REQUIRED onnxtocpp-compile-models-onnx)
  endif()
endif()

# Unit tests for the ONNX parser layer: the tests write the model files
# themselves as hand-crafted protobuf wire format, so they need no Python
onnxtocpp_add_gtest(TestONNXToCppParser TestONNXToCppParser.cxx)

# Unit tests for the safetensors weight file format. nlohmann_json is used by
# the test itself to validate the structure of the generated file.
if(TARGET nlohmann_json::nlohmann_json)
  onnxtocpp_add_gtest(TestONNXToCppSafetensorsWeights TestONNXToCppSafetensorsWeights.cxx
                      LIBRARIES nlohmann_json::nlohmann_json)
else()
  message(STATUS "nlohmann_json not found: the ONNXToCpp safetensors test is disabled")
endif()

if(ONNXTOCPP_TORCH_FOUND AND ONNXTOCPP_ONNX_FOUND AND NOT broken_onnx AND BLAS_FOUND)
  # ModelGeneratorUtils.py holds the code shared by the generators below, so it
  # has to be copied next to them.
  configure_file(ModelGeneratorUtils.py ModelGeneratorUtils.py COPYONLY)
  foreach(generator Conv1d Conv2d Conv3d ConvTrans2d Linear Recurrent)
    configure_file(${generator}ModelGenerator.py ${generator}ModelGenerator.py COPYONLY)
  endforeach()

  onnxtocpp_add_gtest(TestONNXToCppModels TestONNXToCppModels.cxx)
endif()
