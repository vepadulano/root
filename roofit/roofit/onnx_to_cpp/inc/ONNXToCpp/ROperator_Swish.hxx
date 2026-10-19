#ifndef ONNXTOCPP_ROPERATOR_Swish
#define ONNXTOCPP_ROPERATOR_Swish

#include "ONNXToCpp/common.hxx"
#include "ONNXToCpp/ROperator.hxx"
#include "ONNXToCpp/RModel.hxx"

#include <sstream>

namespace ROOT{
namespace Internal{
namespace ONNXToCpp{

class ROperator_Swish final : public ROperator
{

private:

   std::string fNX;
   std::string fNY;
   std::vector<size_t> fShape;

public:
   ROperator_Swish(){}
   ROperator_Swish(std::string nameX, std::string nameY):
      fNX(UTILITY::Clean_name(nameX)), fNY(UTILITY::Clean_name(nameY)){
         fInputTensorNames = { fNX };
         fOutputTensorNames = { fNY };
      }

   void Initialize(RModel& model) override {
      if (model.CheckIfTensorAlreadyExist(fNX) == false){   //input must be a graph input, or already initialized intermediate tensor
         throw std::runtime_error("Internal::ONNXToCpp Swish Op Input Tensor is not found in model");
      }
      fShape = model.GetTensorShape(fNX);
      model.AddIntermediateTensor(fNY, model.GetTensorType(fNX), fShape);
   }


   std::string Generate(std::string OpName) override {
      OpName = "op_" + OpName;
      if (fShape.empty()){
         throw std::runtime_error("Internal::ONNXToCpp Operator Swish called to Generate without being initialized first");
      }
      std::stringstream out;
      int length = 1;
      for(auto& i: fShape){
         length *= i;
      }
      out << "\t" << "for (int id = 0; id < " << length << " ; id++){\n";
      out << "\t\t" << "tensor_" << fNY << "[id] = tensor_" << fNX <<"[id] / (1 + std::exp( - tensor_"  << fNX << "[id]));\n";
      out << "\t}\n";
      return out.str();
   }

   std::vector<std::string> GetStdLibs() override { return { std::string("cmath") };}
};

}//ONNXToCpp
}//Internal
}//ROOT


#endif //ONNXTOCPP_ROPERATOR_Swish
