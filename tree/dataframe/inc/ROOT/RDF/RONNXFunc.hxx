#ifndef ROOT_RDF_RONNXFUNC
#define ROOT_RDF_RONNXFUNC

#include <array>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ROOT::Internal::RDF {

/**
\class ROOT::Internal::RDF::RONNXModel
\brief An ONNX model compiled with the interpreter, together with the sessions to evaluate it.

This is the implementation of ROOT::RDF::Experimental::RONNXFunc, which doesn't depend on the type and on the number
of the columns.
*/
class RONNXModel {
   struct Impl;
   // RDataFrame copies the callables that are passed to Define: the argument is taken by value, and the nodes
   // of the computation graph are cloned together with their callable, for example to apply a systematic
   // variation. All the copies must use the same compiled model and the same sessions, since compiling the
   // model again for every copy would be slow and wasteful. The state is then shared by the copies with a
   // std::shared_ptr. A std::unique_ptr would make the functor move-only, so it couldn't be passed to Define as
   // an lvalue, and it would not be possible to clone the graph nodes.
   std::shared_ptr<Impl> fImpl;

public:
   /// \param[in] onnxFile Path to the ONNX model file.
   /// \throws std::runtime_error If the file can't be read, or if the model is not supported.
   explicit RONNXModel(std::string_view onnxFile);

   /// The number of values of the input of the model, in all of its input tensors.
   std::size_t GetInputSize() const;

   /// Evaluate the model. The evaluation is thread-safe.
   /// \param[in] input The flattened values of the input tensors of the model, which are GetInputSize().
   /// \return The flattened values of the output tensor of the model.
   std::vector<float> Evaluate(const float *input) const;
};

} // namespace ROOT::Internal::RDF

namespace ROOT::RDF::Experimental {

// The call operator of RONNXFunc must have exactly N parameters of type T, so that RDataFrame can deduce the types of
// the columns from its signature. C++ has no direct way to write N parameters when N is a template argument, so the
// class uses a pack with N elements, which comes from the third template parameter. By default it is
// std::make_index_sequence<N>, which is the type std::index_sequence<0, 1, ..., N - 1>, so the users don't need to
// write it. The partial specialization below binds the elements of the sequence to the pack Is.
template <typename T, std::size_t N, typename Indices = std::make_index_sequence<N>>
class RONNXFunc;

/**
\class ROOT::RDF::Experimental::RONNXFunc
\ingroup dataframe
\brief Functor to evaluate an ONNX model in an RDataFrame Define node.

RONNXFunc reads an ONNX model from a file and evaluates it on the values of the
columns of an RDataFrame. The model must have statically-shaped float input
tensors and a single float output tensor.

The model is converted to C++ code that is compiled by the interpreter when the
functor is created, so that the evaluation of an event is a plain compiled
function call. No other package is needed to evaluate the model.

\tparam T The type of the columns that are the input of the model, which is the same for all of them.
\tparam N The number of columns, which must be the number of values of the input of the model. If the model has
more than one input tensor, the input is the values of the first tensor, followed by the ones of the second and so
on.

The functor takes the values of the columns, and returns a `std::vector<float>` with the flattened values of the
output tensor of the model.

The functor can be copied, and the copies share the same model. The evaluation
is thread-safe, so the functor can be used together with ROOT::EnableImplicitMT().

### Example in C++
~~~{.cpp}
ROOT::RDF::Experimental::RONNXFunc<float, 3> model{"model.onnx"};

ROOT::RDataFrame df{"tree", "file.root"};
auto dfWithOutput = df.Define("y", model, {"a", "b", "c"});
~~~

### Example in Python
~~~{.py}
model = ROOT.RDF.Experimental.RONNXFunc["float", 3]("model.onnx")

df = ROOT.RDataFrame("tree", "file.root")
df = df.Define("y", model, ["a", "b", "c"])
~~~
*/
template <typename T, std::size_t N, std::size_t... Is>
class RONNXFunc<T, N, std::index_sequence<Is...>> {
   static_assert(N > 0, "RONNXFunc needs at least one input column");

   // Maps every element of the pack Is to T, so that the expansion of AlwaysT<Is>... is a list of N parameters of type
   // T: for N = 3, (AlwaysT<0>, AlwaysT<1>, AlwaysT<2>) is (T, T, T). The values of the indices are not used, they
   // are only needed to have a pack with N elements to expand. A call operator that is a template, for example one
   // with a variadic pack of types, would not work here: RDataFrame needs a call operator that is not a template to
   // know the number and the types of the columns.
   template <std::size_t>
   using AlwaysT = T;

   ROOT::Internal::RDF::RONNXModel fModel;

public:
   /// \param[in] onnxFile Path to the ONNX model file.
   /// \throws std::runtime_error If the file can't be read, or if the model is not supported.
   /// \throws std::invalid_argument If the number of values of the input of the model is not N.
   explicit RONNXFunc(std::string_view onnxFile) : fModel{onnxFile}
   {
      if (fModel.GetInputSize() != N)
         throw std::invalid_argument("RONNXFunc: the model has " + std::to_string(fModel.GetInputSize()) +
                                     " input values, but the functor has " + std::to_string(N) + " input columns");
   }

   /// Evaluate the model.
   /// \param[in] inputs The values of the columns.
   /// \return The flattened values of the output tensor of the model.
   std::vector<float> operator()(AlwaysT<Is>... inputs) const
   {
      // The pack of the parameters is expanded again here, to convert every column to float
      const std::array<float, N> input{static_cast<float>(inputs)...};
      return fModel.Evaluate(input.data());
   }
};

} // namespace ROOT::RDF::Experimental

#endif // ROOT_RDF_RONNXFUNC
