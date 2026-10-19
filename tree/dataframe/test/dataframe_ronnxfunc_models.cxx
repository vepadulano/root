// Writes the files used by the tests of RONNXFunc, both the C++ and the Python ones:
//  - dataframe_ronnxfunc_gemm.onnx: one input tensor of 4 values, X, and one output tensor of 2 values, Y = X W + B,
//    where W is a matrix with 4 rows and 2 columns with the elements W[i][j] = 2 * i + j + 1, and B = {0.5, -0.5}
//  - dataframe_ronnxfunc_sub.onnx: two input tensors of 3 values, X and Y, and one output tensor Z = X - Y
//  - dataframe_ronnxfunc_two_outputs.onnx: two output tensors, which is not supported
//  - dataframe_ronnxfunc_empty.onnx: an empty file, which is not a valid model
//
// The models are written with the wire format of protobuf, so that no other package is needed to create them.

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

void AppendVarint(std::string &out, std::uint64_t value)
{
   while (value >= 0x80) {
      out.push_back(static_cast<char>((value & 0x7f) | 0x80));
      value >>= 7;
   }
   out.push_back(static_cast<char>(value));
}

void AppendVarintField(std::string &out, int field, std::uint64_t value)
{
   AppendVarint(out, std::uint64_t(field) << 3); // wire type 0: varint
   AppendVarint(out, value);
}

void AppendBytesField(std::string &out, int field, const std::string &payload)
{
   AppendVarint(out, std::uint64_t(field) << 3 | 2); // wire type 2: length-delimited
   AppendVarint(out, payload.size());
   out += payload;
}

// TensorProto of floats, with the values as little-endian raw data
std::string
FloatTensor(const std::string &name, const std::vector<std::uint64_t> &shape, const std::vector<float> &values)
{
   std::string out;
   for (auto dim : shape)
      AppendVarintField(out, 1, dim); // dims
   AppendVarintField(out, 2, 1);      // data_type: FLOAT
   AppendBytesField(out, 8, name);
   std::string rawData;
   for (float value : values) {
      std::uint32_t bits{};
      std::memcpy(&bits, &value, sizeof(bits));
      for (int i = 0; i < 4; ++i)
         rawData.push_back(static_cast<char>((bits >> (8 * i)) & 0xff));
   }
   AppendBytesField(out, 9, rawData);
   return out;
}

// ValueInfoProto of a float tensor
std::string FloatValueInfo(const std::string &name, const std::vector<std::uint64_t> &shape)
{
   std::string shapeProto;
   for (auto dim : shape) {
      std::string dimProto;
      AppendVarintField(dimProto, 1, dim); // dim_value
      AppendBytesField(shapeProto, 1, dimProto);
   }
   std::string tensorType;
   AppendVarintField(tensorType, 1, 1); // elem_type: FLOAT
   AppendBytesField(tensorType, 2, shapeProto);
   std::string type;
   AppendBytesField(type, 1, tensorType);
   std::string out;
   AppendBytesField(out, 1, name);
   AppendBytesField(out, 2, type);
   return out;
}

std::string Node(const std::string &opType, const std::vector<std::string> &inputs, const std::string &output)
{
   std::string node;
   for (const auto &input : inputs)
      AppendBytesField(node, 1, input);
   AppendBytesField(node, 2, output);
   AppendBytesField(node, 4, opType);
   return node;
}

bool WriteFile(const std::string &fileName, const std::string &content)
{
   std::ofstream file{fileName, std::ios::binary};
   file.write(content.data(), static_cast<std::streamsize>(content.size()));
   return file.good();
}

bool WriteModel(const std::string &fileName, const std::string &graph)
{
   std::string model;
   AppendVarintField(model, 1, 8);    // ir_version
   AppendBytesField(model, 7, graph); // graph
   return WriteFile(fileName, model);
}

bool WriteGemmModel(const std::string &fileName)
{
   std::vector<float> weights;
   for (std::uint64_t i = 0; i < 4; ++i)
      for (std::uint64_t j = 0; j < 2; ++j)
         weights.push_back(static_cast<float>(2 * i + j + 1));

   std::string graph;
   AppendBytesField(graph, 1, Node("Gemm", {"X", "W", "B"}, "Y"));
   AppendBytesField(graph, 2, "gemm");
   AppendBytesField(graph, 5, FloatTensor("W", {4, 2}, weights));
   AppendBytesField(graph, 5, FloatTensor("B", {2}, {0.5f, -0.5f}));
   AppendBytesField(graph, 11, FloatValueInfo("X", {1, 4}));
   AppendBytesField(graph, 12, FloatValueInfo("Y", {1, 2}));
   return WriteModel(fileName, graph);
}

bool WriteSubModel(const std::string &fileName)
{
   std::string graph;
   AppendBytesField(graph, 1, Node("Sub", {"X", "Y"}, "Z"));
   AppendBytesField(graph, 2, "sub");
   AppendBytesField(graph, 11, FloatValueInfo("X", {1, 3}));
   AppendBytesField(graph, 11, FloatValueInfo("Y", {1, 3}));
   AppendBytesField(graph, 12, FloatValueInfo("Z", {1, 3}));
   return WriteModel(fileName, graph);
}

bool WriteTwoOutputsModel(const std::string &fileName)
{
   std::string graph;
   AppendBytesField(graph, 1, Node("Add", {"X", "W"}, "Y"));
   AppendBytesField(graph, 1, Node("Mul", {"X", "W"}, "Z"));
   AppendBytesField(graph, 2, "two_outputs");
   AppendBytesField(graph, 5, FloatTensor("W", {1, 3}, {1.f, 2.f, 3.f}));
   AppendBytesField(graph, 11, FloatValueInfo("X", {1, 3}));
   AppendBytesField(graph, 12, FloatValueInfo("Y", {1, 3}));
   AppendBytesField(graph, 12, FloatValueInfo("Z", {1, 3}));
   return WriteModel(fileName, graph);
}

} // namespace

int main()
{
   const bool ok = WriteGemmModel("dataframe_ronnxfunc_gemm.onnx") && WriteSubModel("dataframe_ronnxfunc_sub.onnx") &&
                   WriteTwoOutputsModel("dataframe_ronnxfunc_two_outputs.onnx") &&
                   WriteFile("dataframe_ronnxfunc_empty.onnx", "");
   if (!ok)
      std::cerr << "Failed to write the models of the tests of RONNXFunc\n";
   return ok ? 0 : 1;
}
