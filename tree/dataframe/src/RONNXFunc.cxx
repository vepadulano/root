#include "ROOT/RDF/RONNXFunc.hxx"

#include <ONNXToCpp/RModel.hxx>
#include <ONNXToCpp/RModelParser_ONNX.hxx>

#include <TInterpreter.h>

#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using CreateFunc_t = void *(*)();
using DestroyFunc_t = void (*)(void *);
using EvalFunc_t = void (*)(void *, float *, const float *);

/// A session of the generated model. The code of the model is compiled by the
/// interpreter, which also provides the function to destroy the session.
using SessionPtr_t = std::unique_ptr<void, DestroyFunc_t>;

std::string ReadFile(std::string_view path)
{
   const std::string pathString{path};
   std::ifstream file{pathString, std::ios::binary};
   if (!file)
      throw std::runtime_error("RONNXFunc: failed to open the file '" + pathString + "'");

   std::ostringstream content;
   content << file.rdbuf();
   if (content.str().empty())
      throw std::runtime_error("RONNXFunc: the file '" + pathString + "' is empty");
   return content.str();
}

/// Generate the C++ code to evaluate the model, in the namespace
/// ROOT::Internal::ONNXToCpp_<modelName>.
std::string GenerateCode(const std::string &onnx, const std::string &modelName)
{
   namespace ONNXToCpp = ROOT::Internal::ONNXToCpp;

   std::istringstream stream{onnx};
   auto model = ONNXToCpp::RModelParser_ONNX{}.Parse(stream, modelName);
   model.Generate(ONNXToCpp::Options::kNoWeightFile);

   std::ostringstream code;
   model.PrintGenerated(code);
   return code.str();
}

auto Interpret(const std::string &expression)
{
   return gInterpreter->ProcessLine((expression + ";").c_str());
}

std::size_t InterpretSize(const std::string &expression)
{
   return static_cast<std::size_t>(Interpret(expression));
}

} // namespace

struct ROOT::Internal::RDF::RONNXModel::Impl {
   explicit Impl(std::string_view onnxFile);

   /// Take a session from the ones that are not in use, or create a new one.
   SessionPtr_t Acquire();
   /// Give back a session, so that it can be used by another evaluation.
   void Release(SessionPtr_t session);

   std::size_t fInputSize = 0;
   std::size_t fOutputSize = 0;
   CreateFunc_t fCreate = nullptr;
   DestroyFunc_t fDestroy = nullptr;
   EvalFunc_t fEval = nullptr;

   std::mutex fMutex;
   std::vector<SessionPtr_t> fFreeSessions;
};

ROOT::Internal::RDF::RONNXModel::Impl::Impl(std::string_view onnxFile)
{
   static std::atomic<unsigned int> counter{0};
   const std::string modelName = "rdf_onnx_func_" + std::to_string(counter++);
   const std::string ns = "ROOT::Internal::ONNXToCpp_" + modelName;

   if (!gInterpreter->Declare(GenerateCode(ReadFile(onnxFile), modelName).c_str()))
      throw std::runtime_error("RONNXFunc: failed to compile the code generated for the model in '" +
                               std::string{onnxFile} + "'");

   // The input of the model is the concatenation of its input tensors
   const auto nInputTensors = InterpretSize("std::size(" + ns + "::inputTensorDims)");
   std::string offset = "0";
   std::string inputs;
   for (std::size_t i = 0; i < nInputTensors; ++i) {
      inputs += ", in + (" + offset + ")";
      offset += " + " + ns + "::inputTensorDims[" + std::to_string(i) + "].total_size()";
   }
   fInputSize = InterpretSize(offset);

   if (InterpretSize("std::size(" + ns + "::outputTensorDims)") != 1)
      throw std::runtime_error("RONNXFunc: only models with a single output tensor are supported");
   fOutputSize = InterpretSize(ns + "::outputTensorDims[0].total_size()");

   // Functions with a signature that doesn't depend on the model
   const std::string functions = "namespace " + ns +
                                 " {\n"
                                 "void *rdf_create() { return new Session(); }\n"
                                 "void rdf_destroy(void *session) { delete static_cast<Session *>(session); }\n"
                                 "void rdf_eval(void *session, float *out, const float *in) {\n"
                                 "   doInfer(*static_cast<Session *>(session)" +
                                 inputs +
                                 ", out);\n"
                                 "}\n"
                                 "} // namespace " +
                                 ns + "\n";
   if (!gInterpreter->Declare(functions.c_str()))
      throw std::runtime_error("RONNXFunc: failed to compile the code to evaluate the model in '" +
                               std::string{onnxFile} + "'");

   fCreate = reinterpret_cast<CreateFunc_t>(Interpret("static_cast<void *(*)()>(" + ns + "::rdf_create)"));
   fDestroy = reinterpret_cast<DestroyFunc_t>(Interpret("static_cast<void (*)(void *)>(" + ns + "::rdf_destroy)"));
   fEval = reinterpret_cast<EvalFunc_t>(
      Interpret("static_cast<void (*)(void *, float *, const float *)>(" + ns + "::rdf_eval)"));
}

SessionPtr_t ROOT::Internal::RDF::RONNXModel::Impl::Acquire()
{
   {
      const std::lock_guard<std::mutex> lock{fMutex};
      if (!fFreeSessions.empty()) {
         auto session = std::move(fFreeSessions.back());
         fFreeSessions.pop_back();
         return session;
      }
   }
   // A session holds the weights and the buffers of the model, so it is not
   // thread-safe: every concurrent evaluation needs its own.
   return SessionPtr_t{fCreate(), fDestroy};
}

void ROOT::Internal::RDF::RONNXModel::Impl::Release(SessionPtr_t session)
{
   const std::lock_guard<std::mutex> lock{fMutex};
   fFreeSessions.push_back(std::move(session));
}

ROOT::Internal::RDF::RONNXModel::RONNXModel(std::string_view onnxFile) : fImpl{std::make_shared<Impl>(onnxFile)} {}

std::size_t ROOT::Internal::RDF::RONNXModel::GetInputSize() const
{
   return fImpl->fInputSize;
}

std::vector<float> ROOT::Internal::RDF::RONNXModel::Evaluate(const float *input) const
{
   std::vector<float> output(fImpl->fOutputSize);
   auto session = fImpl->Acquire();
   fImpl->fEval(session.get(), output.data(), input);
   fImpl->Release(std::move(session));
   return output;
}
