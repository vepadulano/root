from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, List, Union, Dict, Tuple

if TYPE_CHECKING:
    import ROOT
    from ROOT._pythonization._rdataframe import AsNumpyResult
    from DistRDF.PythonMergeables import SnapshotResult


_RDF_REGISTER: Dict[Tuple[uuid.UUID, str], ROOT.RDataFrame] = {}
_ACTIONS_REGISTER: Dict[
    Tuple[uuid.UUID, str],
    Union[ROOT.RDF.RResultPtr, ROOT.RDF.Experimental.RResultMap, SnapshotResult, AsNumpyResult]
] = {}
