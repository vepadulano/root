"""Tests of RONNXFunc. The models are written by dataframe_ronnxfunc_models.cxx, which runs before these tests."""

import unittest

import ROOT

RONNXFunc = ROOT.RDF.Experimental.RONNXFunc

GEMM_MODEL = "dataframe_ronnxfunc_gemm.onnx"
SUB_MODEL = "dataframe_ronnxfunc_sub.onnx"
TWO_OUTPUTS_MODEL = "dataframe_ronnxfunc_two_outputs.onnx"
EMPTY_MODEL = "dataframe_ronnxfunc_empty.onnx"

N_ENTRIES = 1000


def gemm_reference(x):
    """The output of the gemm model, Y = X W + B with W[i][j] = 2 * i + j + 1 and B = {0.5, -0.5}"""
    b = [0.5, -0.5]
    return [b[j] + sum(x[i] * (2 * i + j + 1) for i in range(4)) for j in range(2)]


def entry_input(entry):
    """The input of the gemm model in an entry of the datasets of the tests"""
    return [float(entry), 1.0, 2.0, 3.0]


class RONNXFuncTests(unittest.TestCase):
    """Test the RONNXFunc functor to evaluate an ONNX model in an RDataFrame Define node"""

    def check_outputs_in_dataframe(self, model, column_type):
        """Check the output of the model in all the entries of a dataset, with the columns of the input of the model"""
        df = (
            ROOT.RDataFrame(N_ENTRIES)
            .Define("x0", f"static_cast<{column_type}>(rdfentry_)")
            .Define("x1", f"static_cast<{column_type}>(1)")
            .Define("x2", f"static_cast<{column_type}>(2)")
            .Define("x3", f"static_cast<{column_type}>(3)")
            .Define("y", model, ["x0", "x1", "x2", "x3"])
            .Define("y0", "y[0]")
            .Define("y1", "y[1]")
        )
        y0 = df.Take["float"]("y0")
        y1 = df.Take["float"]("y1")

        # The order of the entries is not guaranteed in multithreaded runs
        expected = [gemm_reference(entry_input(entry)) for entry in range(N_ENTRIES)]
        expected0 = sorted(e[0] for e in expected)
        expected1 = sorted(e[1] for e in expected)
        actual0 = sorted(y0.GetValue())
        actual1 = sorted(y1.GetValue())

        self.assertEqual(len(actual0), N_ENTRIES)
        self.assertEqual(len(actual1), N_ENTRIES)
        for a, r in zip(actual0, expected0):
            self.assertAlmostEqual(a, r, delta=1e-3)
        for a, r in zip(actual1, expected1):
            self.assertAlmostEqual(a, r, delta=1e-3)

    def test_evaluate(self):
        model = RONNXFunc["float", 4](GEMM_MODEL)

        output = model(0.5, -1.0, 2.0, 3.0)
        reference = gemm_reference([0.5, -1.0, 2.0, 3.0])

        self.assertEqual(len(output), len(reference))
        for a, r in zip(output, reference):
            self.assertAlmostEqual(a, r, places=5)

    def test_evaluate_with_double_columns(self):
        model = RONNXFunc["double", 4](GEMM_MODEL)

        output = model(0.5, -1.0, 2.0, 3.0)
        reference = gemm_reference([0.5, -1.0, 2.0, 3.0])

        self.assertEqual(len(output), len(reference))
        for a, r in zip(output, reference):
            self.assertAlmostEqual(a, r, places=5)

    def test_several_input_tensors(self):
        """The input is the values of the first input tensor, followed by the ones of the second"""
        model = RONNXFunc["float", 6](SUB_MODEL)

        output = model(10.0, 20.0, 30.0, 1.0, 2.0, 3.0)

        self.assertEqual(list(output), [9.0, 18.0, 27.0])

    def test_wrong_number_of_input_columns(self):
        """The number of columns must be the number of values of the input of the model"""
        with self.assertRaises(Exception):
            RONNXFunc["float", 3](GEMM_MODEL)
        with self.assertRaises(Exception):
            RONNXFunc["float", 5](GEMM_MODEL)

    def test_missing_file(self):
        with self.assertRaises(Exception):
            RONNXFunc["float", 4]("dataframe_ronnxfunc_does_not_exist.onnx")

    def test_empty_file(self):
        with self.assertRaises(Exception):
            RONNXFunc["float", 3](EMPTY_MODEL)

    def test_several_output_tensors(self):
        with self.assertRaises(Exception):
            RONNXFunc["float", 3](TWO_OUTPUTS_MODEL)

    def test_copies_share_the_model(self):
        """The copies of a functor use the same model, and they stay valid after the original is gone"""
        reference = gemm_reference([1.0, 2.0, 3.0, 4.0])

        model = RONNXFunc["float", 4](GEMM_MODEL)
        copies = [RONNXFunc["float", 4](model), RONNXFunc["float", 4](model)]
        del model

        for copy in copies:
            output = copy(1.0, 2.0, 3.0, 4.0)
            self.assertEqual(len(output), len(reference))
            self.assertAlmostEqual(output[0], reference[0], places=5)
            self.assertAlmostEqual(output[1], reference[1], places=5)

    def test_define_in_dataframe(self):
        self.check_outputs_in_dataframe(RONNXFunc["float", 4](GEMM_MODEL), "float")
        self.check_outputs_in_dataframe(RONNXFunc["double", 4](GEMM_MODEL), "double")

    def test_define_in_dataframe_with_implicit_mt(self):
        """Every thread needs a session of its own, which the functor takes care of"""
        ROOT.EnableImplicitMT(4)
        try:
            self.check_outputs_in_dataframe(RONNXFunc["float", 4](GEMM_MODEL), "float")
        finally:
            ROOT.DisableImplicitMT()

    def test_wrong_number_of_columns_in_define(self):
        """The number of columns passed to Define must be the number of columns of the functor"""
        model = RONNXFunc["float", 4](GEMM_MODEL)
        df = ROOT.RDataFrame(10).Define("x0", "1.f").Define("x1", "2.f")

        with self.assertRaises(Exception):
            df.Define("y", model, ["x0", "x1"])


if __name__ == "__main__":
    unittest.main()
