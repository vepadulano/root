### \file
### \ingroup tutorial_ml
### \notebook -nodraw
### Example of inference with ONNX and RDataFrame, of a model trained with PyTorch.
### First, generate the input ONNX model by running `ml_dataloader_training_higgs_dataframe_inference_onnx.py`.
###
### The model is evaluated with ROOT.RDF.Experimental.RONNXFunc, a functor that
### can be used in a Define node of RDataFrame. The columns that are the input
### of the model are passed to Define, in the order of the input tensor of the
### model.
###
### \macro_code
### \macro_output

from os.path import exists

import ROOT

# Check if the input file exists
modelFile = "HiggsModel.onnx"

if not exists(modelFile):
    raise FileNotFoundError(
        "You need to run ml_dataloader_training_higgs_dataframe_inference_onnx.py to generate the ONNX trained model"
    )

# The model has 7 input variables, which are float columns. The functor can be
# used by several threads at the same time.
inputColumns = ["m_jj", "m_jjj", "m_lv", "m_jlv", "m_bb", "m_wbb", "m_wwbb"]
model = ROOT.RDF.Experimental.RONNXFunc["float", len(inputColumns)](modelFile)

# Run inference over input data
inputFile = str(ROOT.gROOT.GetTutorialDir()) + "/machine_learning/data/Higgs_data.root"
df1 = ROOT.RDataFrame("sig_tree", inputFile)
h1 = df1.Define("DNN_Values", model, inputColumns).Define("y", "DNN_Values[0]").Histo1D(("h_sig", "", 100, 0, 1), "y")

df2 = ROOT.RDataFrame("bkg_tree", inputFile)
h2 = df2.Define("DNN_Values", model, inputColumns).Define("y", "DNN_Values[0]").Histo1D(("h_bkg", "", 100, 0, 1), "y")

# Run over the input data once, combining both RDataFrame graphs.
ROOT.RDF.RunGraphs([h1, h2])

print("Number of signal entries", h1.GetEntries())
print("Number of background entries", h2.GetEntries())

h1.SetLineColor("kRed")
h2.SetLineColor("kBlue")

c1 = ROOT.TCanvas()
ROOT.gStyle.SetOptStat(0)

h2.DrawClone()
h1.DrawClone("SAME")
