#ifndef ONNXTOCPP_ROPERATOR_Sigmoid
#define ONNXTOCPP_ROPERATOR_Sigmoid

#include "ONNXToCpp/common.hxx"
#include "ONNXToCpp/ROperator.hxx"
#include "ONNXToCpp/RModel.hxx"

#include <sstream>

namespace ROOT{
namespace Internal{
namespace ONNXToCpp{

template <typename T>
class ROperator_Sigmoid final : public ROperator
{

private:

   std::string fNX;
   std::string fNY;
   std::vector<Dim> fShape;

public:
   ROperator_Sigmoid(){}
   ROperator_Sigmoid(std::string nameX, std::string nameY):
      fNX(UTILITY::Clean_name(nameX)), fNY(UTILITY::Clean_name(nameY)){
         fInputTensorNames = { fNX };
         fOutputTensorNames = { fNY };
      }

   void Initialize(RModel& model) override {
      if (model.CheckIfTensorAlreadyExist(fNX) == false){   //input must be a graph input, or already initialized intermediate tensor
         throw std::runtime_error("Internal::ONNXToCpp Sigmoid Op Input Tensor is not found in model");
      }
      fShape = model.GetDimTensorShape(fNX);
      model.AddIntermediateTensor(fNY, model.GetTensorType(fNX), fShape);
   }


   std::string Generate(std::string opName) override {
      if (fShape.empty()){
         throw std::runtime_error("Internal::ONNXToCpp Operator Sigmoid called to Generate without being initialized first");
      }
      std::stringstream out;
      auto length = ConvertDimShapeToLength(fShape);
      out << "\n//------ Sigmoid -- " << opName << "\n";
      out << SP << "for (int id = 0; id < " << length << " ; id++){\n";
      out << SP << SP  << "tensor_" << fNY << "[id] = 1 / (1 + std::exp( - tensor_"  << fNX << "[id]));\n";
      out << SP << "}\n";
      return out.str();
   }

   std::vector<std::string> GetStdLibs() override { return { std::string("cmath") };}
};

}//ONNXToCpp
}//Internal
}//ROOT


#endif //ONNXTOCPP_ROPERATOR_Sigmoid
