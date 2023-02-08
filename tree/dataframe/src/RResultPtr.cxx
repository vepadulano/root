// Author: Vincenzo Eduardo Padulano, Enrico Guiraud CERN 2023/02

/*************************************************************************
 * Copyright (C) 1995-2023, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#include <string>

#include <ROOT/RResultPtr.hxx>
#include <ROOT/RDF/RInterface.hxx>
#include <ROOT/RDF/RLoopManager.hxx>

using SnapshotPtr_t = ROOT::RDF::RResultPtr<ROOT::RDF::RInterface<ROOT::Detail::RDF::RLoopManager, void>>;
/**
 * \brief Overload for cloning an RResultPtr connected to a Snapshot.
 *
 * \param inptr The pointer.
 * \param newNameForSnapshot A new name for the output file of the cloned action.
 * \return A new pointer with a cloned action.
 */
SnapshotPtr_t ROOT::Internal::RDF::CloneResultPtr(const SnapshotPtr_t &inptr, const std::string &newNameForSnapshot)
{
   return SnapshotPtr_t{
      inptr.fObjPtr, inptr.fLoopManager,
      inptr.fActionPtr->CloneAction(reinterpret_cast<void *>(const_cast<std::string *>(&newNameForSnapshot)))};
}