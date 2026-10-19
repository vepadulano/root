// Tests of RONNXFunc. The models are written by dataframe_ronnxfunc_models.cxx, which runs before these tests.

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RDF/RONNXFunc.hxx>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <stdexcept>
#include <string>
#include <vector>

using ROOT::RDF::Experimental::RONNXFunc;

namespace {

const std::string gGemmModel = "dataframe_ronnxfunc_gemm.onnx";
const std::string gSubModel = "dataframe_ronnxfunc_sub.onnx";
const std::string gTwoOutputsModel = "dataframe_ronnxfunc_two_outputs.onnx";
const std::string gEmptyModel = "dataframe_ronnxfunc_empty.onnx";

constexpr std::size_t gNEntries = 1000;

// The output of the gemm model, Y = X W + B with W[i][j] = 2 * i + j + 1 and B = {0.5, -0.5}
std::array<double, 2> GemmReference(const std::array<double, 4> &x)
{
   std::array<double, 2> y{0.5, -0.5};
   for (std::size_t i = 0; i < 4; ++i)
      for (std::size_t j = 0; j < 2; ++j)
         y[j] += x[i] * static_cast<double>(2 * i + j + 1);
   return y;
}

// The input of the gemm model in an entry of the datasets of the tests
std::array<double, 4> EntryInput(std::size_t entry)
{
   return {static_cast<double>(entry), 1., 2., 3.};
}

// Check the output of the model in all the entries of a dataset, with the columns of the input of the model
template <typename T>
void CheckOutputsInDataFrame(const RONNXFunc<T, 4> &model)
{
   // The values of the output of the model that are checked
   auto firstValue = [](const std::vector<float> &y) { return y[0]; };
   auto secondValue = [](const std::vector<float> &y) { return y[1]; };

   ROOT::RDataFrame df{gNEntries};
   auto withOutput = df.Define("x0", [](ULong64_t entry) { return static_cast<T>(entry); }, {"rdfentry_"})
                        .Define("x1", [] { return static_cast<T>(1); })
                        .Define("x2", [] { return static_cast<T>(2); })
                        .Define("x3", [] { return static_cast<T>(3); })
                        .Define("y", model, {"x0", "x1", "x2", "x3"})
                        .Define("y0", firstValue, {"y"})
                        .Define("y1", secondValue, {"y"});
   auto y0 = withOutput.template Take<float>("y0");
   auto y1 = withOutput.template Take<float>("y1");

   // The order of the entries is not guaranteed in multithreaded runs
   std::vector<float> expected0;
   std::vector<float> expected1;
   for (std::size_t entry = 0; entry < gNEntries; ++entry) {
      const auto reference = GemmReference(EntryInput(entry));
      expected0.push_back(static_cast<float>(reference[0]));
      expected1.push_back(static_cast<float>(reference[1]));
   }
   auto actual0 = y0.GetValue();
   auto actual1 = y1.GetValue();
   std::sort(expected0.begin(), expected0.end());
   std::sort(expected1.begin(), expected1.end());
   std::sort(actual0.begin(), actual0.end());
   std::sort(actual1.begin(), actual1.end());

   ASSERT_EQ(actual0.size(), gNEntries);
   ASSERT_EQ(actual1.size(), gNEntries);
   for (std::size_t i = 0; i < gNEntries; ++i) {
      EXPECT_NEAR(actual0[i], expected0[i], 1e-3);
      EXPECT_NEAR(actual1[i], expected1[i], 1e-3);
   }
}

} // namespace

TEST(RONNXFunc, Evaluate)
{
   const RONNXFunc<float, 4> model{gGemmModel};

   const auto output = model(0.5f, -1.f, 2.f, 3.f);
   const auto reference = GemmReference({0.5, -1., 2., 3.});

   ASSERT_EQ(output.size(), reference.size());
   for (std::size_t i = 0; i < output.size(); ++i)
      EXPECT_NEAR(output[i], reference[i], 1e-5);
}

TEST(RONNXFunc, EvaluateWithDoubleColumns)
{
   const RONNXFunc<double, 4> model{gGemmModel};

   const auto output = model(0.5, -1., 2., 3.);
   const auto reference = GemmReference({0.5, -1., 2., 3.});

   ASSERT_EQ(output.size(), reference.size());
   for (std::size_t i = 0; i < output.size(); ++i)
      EXPECT_NEAR(output[i], reference[i], 1e-5);
}

// The input is the values of the first input tensor, followed by the ones of the second
TEST(RONNXFunc, SeveralInputTensors)
{
   const RONNXFunc<float, 6> model{gSubModel};

   const auto output = model(10.f, 20.f, 30.f, 1.f, 2.f, 3.f);

   ASSERT_EQ(output.size(), 3u);
   EXPECT_FLOAT_EQ(output[0], 9.f);
   EXPECT_FLOAT_EQ(output[1], 18.f);
   EXPECT_FLOAT_EQ(output[2], 27.f);
}

// The number of columns must be the number of values of the input of the model
TEST(RONNXFunc, WrongNumberOfInputColumnsThrows)
{
   EXPECT_THROW((RONNXFunc<float, 3>{gGemmModel}), std::invalid_argument);
   EXPECT_THROW((RONNXFunc<float, 5>{gGemmModel}), std::invalid_argument);
}

TEST(RONNXFunc, MissingFileThrows)
{
   EXPECT_THROW((RONNXFunc<float, 4>{"dataframe_ronnxfunc_does_not_exist.onnx"}), std::runtime_error);
}

TEST(RONNXFunc, EmptyFileThrows)
{
   EXPECT_THROW((RONNXFunc<float, 3>{gEmptyModel}), std::runtime_error);
}

TEST(RONNXFunc, SeveralOutputTensorsThrow)
{
   EXPECT_THROW((RONNXFunc<float, 3>{gTwoOutputsModel}), std::runtime_error);
}

// The copies of a functor use the same model, and they stay valid after the original is gone
TEST(RONNXFunc, CopiesShareTheModel)
{
   const auto reference = GemmReference({1., 2., 3., 4.});

   std::vector<RONNXFunc<float, 4>> copies;
   {
      const RONNXFunc<float, 4> model{gGemmModel};
      copies.push_back(model);
      copies.push_back(model);
   }

   for (const auto &copy : copies) {
      const auto output = copy(1.f, 2.f, 3.f, 4.f);
      ASSERT_EQ(output.size(), reference.size());
      EXPECT_NEAR(output[0], reference[0], 1e-5);
      EXPECT_NEAR(output[1], reference[1], 1e-5);
   }
}

TEST(RONNXFunc, DefineInDataFrame)
{
   CheckOutputsInDataFrame(RONNXFunc<float, 4>{gGemmModel});
   CheckOutputsInDataFrame(RONNXFunc<double, 4>{gGemmModel});
}

#ifdef R__USE_IMT
// Every thread needs a session of its own, which the functor takes care of
TEST(RONNXFunc, DefineInDataFrameWithImplicitMT)
{
   ROOT::EnableImplicitMT(4);
   CheckOutputsInDataFrame(RONNXFunc<float, 4>{gGemmModel});
   ROOT::DisableImplicitMT();
}
#endif

// The number of columns passed to Define must be the number of columns of the functor
TEST(RONNXFunc, WrongNumberOfColumnsInDefineThrows)
{
   const RONNXFunc<float, 4> model{gGemmModel};
   ROOT::RDataFrame df{10};
   auto withColumns = df.Define("x0", [] { return 1.f; }).Define("x1", [] { return 2.f; });

   EXPECT_THROW(withColumns.Define("y", model, {"x0", "x1"}), std::runtime_error);
}
