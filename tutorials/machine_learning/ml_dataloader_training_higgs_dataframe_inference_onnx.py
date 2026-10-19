### \file
### \ingroup tutorial_ml
### \notebook -nodraw
### This macro trains a simple deep neural network on the Higgs dataset with
### PyTorch, exports the model to ONNX and checks that the ONNX model evaluated
### with ROOT::RDF::Experimental::RONNXFunc gives the same output as PyTorch.
### The batches of events to train the model are read from the dataset with
### ROOT.Experimental.ML.RDataLoader.
###
### The trained model is saved as HiggsModel.onnx and is used as input by the
### other tutorials of inference with ONNX and RDataFrame (e.g.
### ml_dataframe_inference_onnx.C), so this macro needs to be run before them.
###
### \macro_code
### \macro_output

import inspect

import ROOT
import torch
import torch.nn as nn

# The input variables of the model, in the order of the input tensor
INPUT_VARIABLES = ["m_jj", "m_jjj", "m_lv", "m_jlv", "m_bb", "m_wbb", "m_wwbb"]


def PrepareData(batch_size=50):
    # get the input data
    inputFile = str(ROOT.gROOT.GetTutorialDir()) + "/machine_learning/data/Higgs_data.root"

    sig = ROOT.RDataFrame("sig_tree", inputFile)
    bkg = ROOT.RDataFrame("bkg_tree", inputFile)

    # The data loader reads the signal and the background together. The column
    # Type is the target, which is 1 for the signal and 0 for the background.
    # The dataset is small, so it is loaded in memory all at once: the events of
    # the two classes are then mixed in the batches by the shuffling.
    dataloader = ROOT.Experimental.ML.RDataLoader(
        [sig, bkg],
        batch_size=batch_size,
        columns=INPUT_VARIABLES + ["Type"],
        target="Type",
        shuffle=True,
        set_seed=1234,
        drop_remainder=True,
        load_eager=True,
    )

    # split data in training and test data
    return dataloader.train_test_split(test_size=0.5)


def CreateModel(nlayers=4, nunits=64):
    layers = []
    ninputs = len(INPUT_VARIABLES)
    for i in range(1, nlayers):
        layers += [nn.Linear(ninputs, nunits), nn.ReLU()]
        ninputs = nunits
    layers += [nn.Linear(ninputs, 1), nn.Sigmoid()]
    model = nn.Sequential(*layers)
    print(model)
    return model


def TrainModel(model, train_batches, epochs=5):
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters())
    for epoch in range(epochs):
        running_loss = 0.0
        nbatches = 0
        for x, y in train_batches.as_torch():
            optimizer.zero_grad()
            loss = criterion(model(x), y)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
            nbatches += 1
        print(f"Epoch {epoch + 1}/{epochs} - average loss: {running_loss / nbatches:.4f}")


def ExportModel(model, modelName):
    # need to evaluate the model before exporting to ONNX
    # and to provide a dummy input tensor to set the input model shape
    # (the batch size is fixed to 1, since the model is evaluated event by event)
    model.eval()

    modelFile = modelName + ".onnx"
    dummy_x = torch.randn(1, len(INPUT_VARIABLES))
    model(dummy_x)

    # check for torch.onnx.export parameters
    def filtered_kwargs(func, **candidate_kwargs):
        sig = inspect.signature(func)
        return {k: v for k, v in candidate_kwargs.items() if k in sig.parameters}

    kwargs = filtered_kwargs(
        torch.onnx.export,
        input_names=["input"],
        output_names=["output"],
        external_data=False,  # may not exist
        dynamo=True,  # may not exist
    )
    print("calling torch.onnx.export with parameters", kwargs)

    torch.onnx.export(model, dummy_x, modelFile, **kwargs)

    print("model exported to ONNX as", modelFile)
    return modelFile


###################################################################
## Step 1 : Create and train the model, export it to ONNX
###################################################################

train_batches, test_batches = PrepareData()

# create dense model with 3 layers of 64 units and train it
model = CreateModel(3, 64)
TrainModel(model, train_batches)
modelFile = ExportModel(model, "HiggsModel")

# evaluate the trained model on the first test events, for comparison with ONNX
x_check, _ = next(iter(test_batches.as_torch()))
x_check = x_check[:10]
with torch.no_grad():
    ytorch = model(x_check).numpy()
x_check = x_check.numpy()

###################################################################
## Step 2 : Evaluate the exported ONNX model
###################################################################

# The functor takes the input variables of the model and returns the output
# of the model. It is the same that is used in the Define nodes of RDataFrame.
onnxModel = ROOT.RDF.Experimental.RONNXFunc["float", len(INPUT_VARIABLES)](modelFile)

for i in range(x_check.shape[0]):
    y = onnxModel(*x_check[i])
    print("input to model is ", x_check[i], "\n\t -> output using ONNX = ", y[0], " using PyTorch = ", ytorch[i, 0])
    if abs(y[0] - ytorch[i, 0]) > 0.01:
        raise RuntimeError("ERROR: Result is different between ONNX and PyTorch")

print("OK")
