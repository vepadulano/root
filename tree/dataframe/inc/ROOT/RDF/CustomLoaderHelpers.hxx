#ifndef ROOT_RDF_CUSTOMLOADERHELPERS
#define ROOT_RDF_CUSTOMLOADERHELPERS

#include <ROOT/RDF/RActionImpl.hxx>
#include <ROOT/RVec.hxx>
#include <ROOT/RDF/RMergeableValue.hxx>

#include <string_view>
#include <memory>
#include <cstring>

class TTreeReader;

namespace ROOT::Internal::RDF {

class R__CLING_PTRCHECK(off) CustomLoaderHelper : public ROOT::Detail::RDF::RActionImpl<CustomLoaderHelper> {
   std::shared_ptr<ROOT::RVecF> fLocation;
   unsigned int fNSlots;

public:
   CustomLoaderHelper(const std::shared_ptr<ROOT::RVecF> &location, const unsigned int nSlots)
      : fLocation(location), fNSlots(nSlots)
   {
   }

   CustomLoaderHelper(CustomLoaderHelper &&) = default;
   CustomLoaderHelper &operator=(CustomLoaderHelper &&) = default;
   CustomLoaderHelper(const CustomLoaderHelper &) = delete;
   CustomLoaderHelper &operator=(const CustomLoaderHelper &) = delete;
   ~CustomLoaderHelper() override = default;

   void InitTask(TTreeReader *, unsigned int) {}
   void Exec([[maybe_unused]] unsigned int slot, const std::vector<void *> &values,
             const std::vector<const std::type_info *> &typeIDs);
   void Initialize() { /* noop */ }
   void Finalize() {}

   // Helper functions for RMergeableValue
   std::unique_ptr<ROOT::Detail::RDF::RMergeableValueBase> GetMergeableValue() const final
   {
      throw std::runtime_error("not implemented.");
   }

   std::string GetActionName() { return "CustomLoaderHelper"; }

   CustomLoaderHelper MakeNew(void *newResult, std::string_view /*variation*/ = "nominal")
   {
      auto &result = *static_cast<std::shared_ptr<ROOT::RVecF> *>(newResult);
      return CustomLoaderHelper(result, fNSlots);
   }
};
} // namespace ROOT::Internal::RDF

#endif // ROOT_RDF_CUSTOMLOADERHELPERS
