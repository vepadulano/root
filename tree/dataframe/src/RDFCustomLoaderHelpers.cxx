#include <ROOT/RDF/CustomLoaderHelpers.hxx>
#include <variant>
void ROOT::Internal::RDF::CustomLoaderHelper::Exec(unsigned int slot, const std::__1::vector<void *> &values,
                                                   const std::__1::vector<const std::type_info *> &typeIDs)
{
   auto append_i = [](std::vector<float> &dest, int i) { dest.push_back(i); };
   auto append_l = [](std::vector<float> &dest, long l) { dest.push_back(l); };
   auto append_ull = [](ROOT::RVec<float> &dest, void *ull) {
      dest.push_back(*reinterpret_cast<unsigned long long *>(ull));
   };
   auto append_f = [](ROOT::RVec<float> &dest, void *ull) { dest.push_back(*reinterpret_cast<float *>(ull)); };
   std::variant<decltype(append_i), decltype(append_l), decltype(append_ull)> var;
   var = append_ull;
   // All values should be convertible to float
   auto nValues{values.size()};
   for (decltype(nValues) i{}; i < nValues; i++) {
      // if (*typeIDs[i] == typeid(int))
      //    fLocation->push_back(*static_cast<int *>(values[i]));
      // else if(*typeIDs[i] == typeid(float))
      //    fLocation->push_back(*static_cast<float *>(values[i]));
      // else if (*typeIDs[i] == typeid(unsigned long long))
      //    fLocation->push_back(*static_cast<unsigned long long *>(values[i]));
      std::get<2>(var)(*fLocation, values[i]);
   }
}
