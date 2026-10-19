/// \file
/// \ingroup tutorial_ml
/// \notebook -nodraw
/// This macro provides an example of using a trained model with PyTorch
/// and make inference using ONNX and RDataFrame.
/// This macro uses as input an ONNX model generated with the
/// Python tutorial ml_dataloader_training_higgs_dataframe_inference_onnx.py
/// You need to run that macro before to generate the trained PyTorch model
/// exported to ONNX.
///
/// The model is evaluated with ROOT::RDF::Experimental::RONNXFunc, a functor
/// that can be used in a Define node of RDataFrame. The columns that are the
/// input of the model are passed to Define, in the order of the input tensor
/// of the model.
///
/// Execute in this order:
/// ```
/// python3 ml_dataloader_training_higgs_dataframe_inference_onnx.py
/// root ml_dataframe_inference_onnx.C
/// ```
///
/// \macro_code
/// \macro_output

auto c1 = std::make_unique<TCanvas>();

void ml_dataframe_inference_onnx(int nthreads = 2)
{
   // check if the input file exists
   const std::string modelFile = "HiggsModel.onnx";
   if (gSystem->AccessPathName(modelFile.c_str())) {
      Error("ml_dataframe_inference_onnx",
            "You need to run ml_dataloader_training_higgs_dataframe_inference_onnx.py to generate the ONNX trained "
            "model");
      return;
   }

   std::string inputFileName = "Higgs_data.root";
   std::string inputFile = std::string{gROOT->GetTutorialDir()} + "/machine_learning/data/" + inputFileName;

   ROOT::EnableImplicitMT(nthreads);

   // The model has 7 input variables, which are float columns. The functor can
   // be used by several threads at the same time.
   ROOT::RDF::Experimental::RONNXFunc<float, 7> model{modelFile};
   const std::vector<std::string> inputColumns{"m_jj", "m_jjj", "m_lv", "m_jlv", "m_bb", "m_wbb", "m_wwbb"};

   // The model returns a vector with one value, which is what is plotted
   auto firstValue = [](const std::vector<float> &values) { return values[0]; };

   ROOT::RDataFrame df1("sig_tree", inputFile);
   std::cout << "Running using " << df1.GetNSlots() << " threads" << std::endl;
   auto h1 = df1.Define("DNN_Values", model, inputColumns)
                .Define("y", firstValue, {"DNN_Values"})
                .Histo1D<float>({"h_sig", "", 100, 0, 1}, "y");

   ROOT::RDataFrame df2("bkg_tree", inputFile);
   auto h2 = df2.Define("DNN_Values", model, inputColumns)
                .Define("y", firstValue, {"DNN_Values"})
                .Histo1D<float>({"h_bkg", "", 100, 0, 1}, "y");

   // Run over the input data once, combining both RDataFrame graphs.
   ROOT::RDF::RunGraphs({h1, h2});

   std::cout << "Number of signal entries " << h1->GetEntries() << "\n";
   std::cout << "Number of background entries " << h2->GetEntries() << "\n";

   h1->SetLineColor(kRed);
   h2->SetLineColor(kBlue);

   gStyle->SetOptStat(0);

   h2->DrawClone();
   h1->DrawClone("SAME");
   c1->BuildLegend();
}
