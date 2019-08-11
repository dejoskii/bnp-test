from twisted.trial import unittest
from bnp import BnpTest
import pandas as pd
import os

class TestBnpSolution(unittest.TestCase):

    def setUp(self):
        self.obj = BnpTest()
        self.xml = "input.xml"
        self.result_csv = "results.csv"
        self.load_file = self.obj.create_df_list(self.xml)

    def test_create_df_list(self):
        self.df_list = self.obj.create_df_list(self.xml)
        df_headers = self.df_list[0].keys()
        self.assertIn("CorrelationID", df_headers)
        self.assertIn("NumberOfTrades", df_headers)
        self.assertIn("Value", df_headers)
        self.assertEqual(self.df_list[4]['CorrelationID'], "200")
        self.assertEqual(self.df_list[4]['NumberOfTrades'], "2")
        self.assertEqual(self.df_list[4]['Limit'], "2000")

    def test_create_df_corid(self):

        corid_list = ["234", "234", "222", "234", "200", "200"]
        test_cordid_list = self.obj.create_df_series()[0]
        self.assertListEqual(corid_list, test_cordid_list)

    def test_create_series(self):

        expected_df = pd.read_csv("expected_df.csv") # The data frame series are integers at this point
        expected_df=expected_df.astype(str)  # convert everything to string
        df_drop = expected_df.columns.drop(["CorrelationID", "TradeID"])  # drop columns that we need to remain strings
        # convert the rest to integers
        expected_df[df_drop] = expected_df[df_drop].apply(pd.to_numeric, errors ="coerce")
        print(expected_df)
        print(type(expected_df.iloc[0, 0]))
        print(type(expected_df.iloc[0,3]))
        my_df = self.obj.create_df_series()[1]
        pd.testing.assert_frame_equal(my_df, expected_df)

    def test_aggregate_df_corid(self):
        aggr_list = []
        aggr_1 = pd.read_csv("data1.csv")
        aggr_list.append(aggr_1)
        aggr_2 = pd.read_csv("data2.csv")
        aggr_list.append(aggr_2)
        aggr_3 = pd.read_csv("data3.csv")
        aggr_list.append(aggr_3)
        expected_aggr_df = pd.concat(aggr_list, ignore_index=True)
        first_df = self.obj.create_df_series()[1]
        second_df = self.obj.aggregate_df_corid()
        final_df = self.obj.write_to_csv("test_output.csv")
        expected_aggr_df = expected_aggr_df.astype(str)
        expected_aggr_drop = expected_aggr_df.columns.drop(["CorrelationID","State"])
        expected_aggr_df[expected_aggr_drop] = expected_aggr_df[expected_aggr_drop].apply(pd.to_numeric)
        pd.testing.assert_frame_equal(final_df, expected_aggr_df)









