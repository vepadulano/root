#include "ONNXToCpp/RModelParser_ONNX.hxx"
#include "ONNXToCpp/ROperator_Not.hxx"
#include "onnx.hxx"

namespace ROOT {
namespace Internal {
namespace ONNXToCpp {

ParserFuncSignature ParseNot = [](RModelParser_ONNX &parser, const onnx::NodeProto &nodeproto) {
   ETensorType input_type = ETensorType::UNDEFINED;

   if (nodeproto.input_size() != 1 || nodeproto.output_size() != 1)
      std::runtime_error("Internal::ONNXToCpp ONNX Parser Not op has invalid input or output size ");

   std::string input_name = nodeproto.input(0);

   if (parser.IsRegisteredTensorType(input_name)) {
      input_type = parser.GetTensorType(input_name);
      if (input_type != ETensorType::BOOL && input_type != ETensorType::UINT8)
         throw std::runtime_error("Internal::ONNXToCpp ONNX Parser Not op has invalid input type " +
                                  ConvertTypeToString(input_type));
   } else {
      throw std::runtime_error("Internal::ONNXToCpp ONNX Parser Not op has input tensor " + input_name +
                               " but its type is not yet registered");
   }

   std::string output_name = nodeproto.output(0);
   std::unique_ptr<ROperator> op(new ROperator_Not(input_name, output_name));

   // Infer the output type
   if (!parser.IsRegisteredTensorType(output_name)) {
      parser.RegisterTensorType(output_name, input_type);
   }

   return op;
};

} // namespace ONNXToCpp
} // namespace Internal
} // namespace ROOT
