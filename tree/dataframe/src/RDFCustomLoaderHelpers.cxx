#include <ROOT/RDF/CustomLoaderHelpers.hxx>
#include <ROOT/RDF/Utils.hxx> // TypeID2TypeName

#include <algorithm>
#include <stdexcept>
#include <typeinfo>

namespace {

using ROOT::Internal::RDF::CustomLoaderHelper;

template <typename T>
void AppendAs(ROOT::RVecF &dest, void *value, std::size_t /*maxSize*/, float /*padding*/)
{
   dest.push_back(static_cast<float>(*static_cast<T *>(value)));
}

template <typename T>
void AppendVectorAs(ROOT::RVecF &dest, void *value, std::size_t maxSize, float padding)
{
   const auto &vec = *static_cast<ROOT::RVec<T> *>(value);
   const auto size = std::min(vec.size(), maxSize);
   dest.insert(dest.end(), vec.begin(), vec.begin() + size);
   dest.insert(dest.end(), maxSize - size, padding);
}

template <typename T>
void AppendAsBulk(ROOT::RVecF &dest, void *valueArray, std::size_t entryInBulk, std::size_t /*maxSize*/,
                  float /*padding*/)
{
   dest.push_back(static_cast<float>(*static_cast<T *>(
      reinterpret_cast<void *>(reinterpret_cast<std::byte *>(valueArray) + entryInBulk * sizeof(T)))));
}

template <typename T>
void AppendVectorAsBulk(ROOT::RVecF &dest, void *valueArray, std::size_t entryInBulk, std::size_t maxSize,
                        float padding)
{
   const auto &vec = *static_cast<ROOT::RVec<T> *>(
      reinterpret_cast<void *>(reinterpret_cast<std::byte *>(valueArray) + entryInBulk * sizeof(ROOT::RVec<T>)));
   const auto size = std::min(vec.size(), maxSize);
   dest.insert(dest.end(), vec.begin(), vec.begin() + size);
   dest.insert(dest.end(), maxSize - size, padding);
}

/// Pick the handler for a column of type T or RVec<T>, flagging vector columns
template <typename T>
bool MatchColHandler(const std::type_info &colType, CustomLoaderHelper::ColHandler_t &handler, bool &isVector)
{
   if (colType == typeid(T)) {
      handler = &AppendAsBulk<T>;
   } else if (colType == typeid(ROOT::RVec<T>)) {
      handler = &AppendVectorAsBulk<T>;
      isVector = true;
   }
   return handler != nullptr;
}

/// Pick the handler matching the type of a column among the supported types and their RVecs
template <typename... Types>
CustomLoaderHelper::ColHandler_t ResolveColHandler(const std::type_info &colType, bool &isVector)
{
   CustomLoaderHelper::ColHandler_t handler = nullptr;
   if ((MatchColHandler<Types>(colType, handler, isVector) || ...))
      return handler;

   throw std::invalid_argument("CustomLoaderHelper: column type '" + ROOT::Internal::RDF::TypeID2TypeName(colType) +
                               "' cannot be converted to float.");
}

} // namespace

ROOT::Internal::RDF::CustomLoaderHelper::CustomLoaderHelper(const std::shared_ptr<ROOT::RVecF> &location,
                                                            const std::vector<const std::type_info *> &colTypeIDs,
                                                            const std::vector<std::size_t> &vecSizes, float vecPadding)
   : fLocation(location), fVecPadding(vecPadding)
{
   fColHandlers.reserve(colTypeIDs.size());
   fMaxSizes.reserve(colTypeIDs.size());
   std::size_t vecIdx = 0;
   for (const auto *colType : colTypeIDs) {
      bool isVector = false;
      fColHandlers.push_back(
         ResolveColHandler<float, double, bool, char, signed char, unsigned char, short, unsigned short, int,
                           unsigned int, long, unsigned long, long long, unsigned long long>(*colType, isVector));
      // Vector columns take the next maximum size, in column order
      fMaxSizes.push_back(isVector ? vecSizes[vecIdx++] : 1);
   }
}

void ROOT::Internal::RDF::CustomLoaderHelper::Exec(unsigned int /*slot*/, const std::vector<void *> &values)
{
   // The readers deliver the values in column order, the same order the handlers were resolved in
   auto nValues{values.size()};
   for (decltype(nValues) i{}; i < nValues; i++)
      fColHandlers[i](*fLocation, values[i], 0, fMaxSizes[i], fVecPadding);
}

void ROOT::Internal::RDF::CustomLoaderHelper::ExecBulk(unsigned int, const std::vector<void *> &values,
                                                       const ROOT::Internal::RDF::RMaskedEntryRange &mask)
{
   // The readers deliver the values in column order, the same order the handlers were resolved in
   // WARNING: this method expects the input values to be beginnings of arrays of contiguous values
   auto nValues{values.size()};
   for (std::size_t entryInBulk = 0; entryInBulk < mask.GetBulkSize(); entryInBulk++) {
      if (mask[entryInBulk]) {
         for (decltype(nValues) colIdx{}; colIdx < nValues; colIdx++)
            fColHandlers[colIdx](*fLocation, reinterpret_cast<std::byte *>(values[colIdx]), entryInBulk,
                                 fMaxSizes[colIdx], fVecPadding);
      }
   }
}
