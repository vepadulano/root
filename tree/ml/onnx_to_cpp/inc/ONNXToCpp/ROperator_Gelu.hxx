#ifndef ONNXTOCPP_ROPERATOR_GELU
#define ONNXTOCPP_ROPERATOR_GELU

#include "ONNXToCpp/common.hxx"
#include "ONNXToCpp/ROperator.hxx"
#include "ONNXToCpp/RModel.hxx"

#include <sstream>

namespace ROOT {
namespace Internal {
namespace ONNXToCpp {

class ROperator_Gelu final : public ROperator {

private:
   std::string fNX;
   std::string fNY;
   std::string fApproximate; // "none" (exact) or "tanh" (approximate)
   std::vector<size_t> fShape;

public:
   ROperator_Gelu() {}
   ROperator_Gelu(std::string nameX, std::string nameY, std::string approximate = "none")
      : fNX(UTILITY::Clean_name(nameX)), fNY(UTILITY::Clean_name(nameY)), fApproximate(approximate)
   {
      fInputTensorNames = {fNX};
      fOutputTensorNames = {fNY};
   }

   void Initialize(RModel &model) override
   {
      if (model.CheckIfTensorAlreadyExist(fNX) == false) {
         throw std::runtime_error("Internal::ONNXToCpp Gelu Op Input Tensor is not found in model");
      }
      fShape = model.GetTensorShape(fNX);
      model.AddIntermediateTensor(fNY, model.GetTensorType(fNX), fShape);
   }

   std::string Generate(std::string OpName) override
   {
      OpName = "op_" + OpName;
      if (fShape.empty()) {
         throw std::runtime_error(
            "Internal::ONNXToCpp Operator Gelu called to Generate without being initialized first");
      }
      std::stringstream out;
      int length = 1;
      for (auto &i : fShape) {
         length *= i;
      }
      out << SP << "for (int id = 0; id < " << length << " ; id++){\n";
      if (fApproximate == "tanh") {
         // Tanh approximation: 0.5 * x * (1 + tanh(sqrt(2/pi) * (x + 0.044715 * x^3)))
         out << SP << SP << "float x = tensor_" << fNX << "[id];\n";
         out << SP << SP << "tensor_" << fNY
             << "[id] = 0.5f * x * (1.0f + std::tanh(0.7978845608028654f * (x + 0.044715f * x * x * x)));\n";
      } else {
         // Exact: 0.5 * x * (1 + erf(x / sqrt(2)))
         out << SP << SP << "tensor_" << fNY << "[id] = 0.5f * tensor_" << fNX << "[id] * (1.0f + std::erf(tensor_"
             << fNX << "[id] * 0.7071067811865475f));\n";
      }
      out << SP << "}\n";
      return out.str();
   }

   std::vector<std::string> GetStdLibs() override { return {std::string("cmath")}; }
};

} // namespace ONNXToCpp
} // namespace Internal
} // namespace ROOT

#endif // ONNXTOCPP_ROPERATOR_GELU
