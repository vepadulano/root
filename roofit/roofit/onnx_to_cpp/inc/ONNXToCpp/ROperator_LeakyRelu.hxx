#ifndef ONNXTOCPP_ROPERATOR_LeakyRelu
#define ONNXTOCPP_ROPERATOR_LeakyRelu

#include "ONNXToCpp/common.hxx"
#include "ONNXToCpp/ROperator.hxx"
#include "ONNXToCpp/RModel.hxx"

#include <sstream>

namespace ROOT{
namespace Internal{
namespace ONNXToCpp{

template <typename T>
class ROperator_LeakyRelu final : public ROperator
{

private:

   /* Attributes*/
   float falpha=0.01; //default value
   std::string fNX;
   std::string fNY;
   std::vector<size_t> fShape;
   std::string fType;

public:
   ROperator_LeakyRelu(){}
   ROperator_LeakyRelu(float alpha,std::string nameX, std::string nameY):
   falpha(alpha),fNX(UTILITY::Clean_name(nameX)), fNY(UTILITY::Clean_name(nameY))
   {
      if(std::is_same<T, float>::value){
         fType = "float";
      }
		else{
			throw
				std::runtime_error("Internal::ONNXToCpp Encountered unsupported type parsing a Leaky Relu operator");
		}

      fInputTensorNames = { fNX };
      fOutputTensorNames = { fNY };
   }

   void Initialize(RModel& model) override {
      if (model.CheckIfTensorAlreadyExist(fNX) == false){   //input must be a graph input, or already initialized intermediate tensor
         throw std::runtime_error("Internal::ONNXToCpp Leaky Relu Op Input Tensor is not found in model");
      }
      fShape = model.GetTensorShape(fNX);
      model.AddIntermediateTensor(fNY, model.GetTensorType(fNX), fShape);
   }


   std::string Generate(std::string OpName) override {
      OpName = "op_" + OpName;
      if (fShape.empty()) {
         throw std::runtime_error("Internal::ONNXToCpp Operator Leaky Relu called to Generate without being initialized first");
      }
      std::stringstream out;
      size_t length = ConvertShapeToLength(fShape);

      out << SP << "constexpr float " << OpName << "_alpha = " << std::setprecision(std::numeric_limits<float>::max_digits10) << falpha << ";\n";

      out << "\n//------ LEAKY RELU\n";
      out << SP << "for (int id = 0; id < " << length << " ; id++){\n";
      out << SP << SP << "tensor_" << fNY << "[id] = ((tensor_" << fNX << "[id] >= 0 )? tensor_" << fNX << "[id] : "<< OpName << "_alpha * tensor_"<< fNX<<"[id]);\n";
      out << SP << "}\n";
      return out.str();
   }

};

}//ONNXToCpp
}//Internal
}//ROOT


#endif //ONNXTOCPP_ROPERATOR_LeakyRelu
