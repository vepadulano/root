#ifndef ONNXTOCPP_ROPERATOR_EyeLike
#define ONNXTOCPP_ROPERATOR_EyeLike

#include "ONNXToCpp/common.hxx"
#include "ONNXToCpp/ROperator.hxx"
#include "ONNXToCpp/RModel.hxx"

#include <sstream>

namespace ROOT {
namespace Internal {
namespace ONNXToCpp {

class ROperator_EyeLike final : public ROperator {

private:
   int fdtype = static_cast<int>(ETensorType::FLOAT); // Default value
   int fk = 0;
   std::string fNX;
   std::string fNY;
   std::vector<size_t> fShape;

public:
   ROperator_EyeLike() {}
   ROperator_EyeLike(int dtype, int k, std::string nameX, std::string nameY)
      : fdtype(dtype), fk(k), fNX(UTILITY::Clean_name(nameX)), fNY(UTILITY::Clean_name(nameY))
   {
      fInputTensorNames = {fNX};
      fOutputTensorNames = {fNY};
   }

   void Initialize(RModel &model) override
   {
      if (model.CheckIfTensorAlreadyExist(fNX) ==
          false) { // input must be a graph input, or already initialized intermediate tensor
         throw std::runtime_error("Internal::ONNXToCpp EyeLike Op Input Tensor is not found in model");
      }
      fShape = model.GetTensorShape(fNX);
      if (fShape.size() != 2)
         throw std::runtime_error("Internal::ONNXToCpp EyeLike Op Input Tensor is not of rank 2");

      if (fdtype) {
         ETensorType extractedType = static_cast<ETensorType>(fdtype);
         model.AddIntermediateTensor(fNY, extractedType, fShape);
      } else {
         model.AddIntermediateTensor(fNY, model.GetTensorType(fNX), fShape);
      }
   }

   std::string Generate(std::string OpName) override
   {
      OpName = "op_" + OpName;
      if (fShape.empty()) {
         throw std::runtime_error(
            "Internal::ONNXToCpp Operator EyeLike called to Generate without being initialized first");
      }
      auto length = ConvertShapeToLength(fShape);
      auto stride = ROOT::Internal::ONNXToCpp::UTILITY::ComputeStrideFromShape(fShape);
      std::stringstream out;
      out << SP << "///--------EyeLike operator\n" << std::endl;
      // add a dummy statement to avoid warning for unused input
      out << SP << "(void) tensor_" << fNX << ";\n";

      out << SP << "std::fill(tensor_" << fNY << ", tensor_" << fNY << " + " << length << "," << 0 << ");\n";
      out << SP << "for (int i = 0; i < " << fShape[0] << "; i++) {\n";
      out << SP << SP << "int j = i +" << fk << ";\n";
      out << SP << SP << "if (j >= 0 && j < " << fShape[1] << ")\n";
      out << SP << SP << SP << "tensor_" << fNY << "[i * " << fShape[1] << "+ j] = 1;\n";
      out << SP << "}\n";
      return out.str();
   }
};

} // namespace ONNXToCpp
} // namespace Internal
} // namespace ROOT

#endif // ONNXTOCPP_ROPERATOR_EyeLike
