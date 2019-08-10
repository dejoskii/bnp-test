import unittest
from bnp import bnpTest
import os

class TestBnpSolution(unittest.TestCase):

    def setUp(self):
        self.obj = bnpTest()

        self.result_csv = 'results.csv'

    def test_create_df_list(self):
        self.df_list = self.obj.create_df_list("input.xml")
        df_headers = self.df_list[0].keys()
        self.assertIn('CorrelationID', df_headers)
        self.assertIn('NumberOfTrades', df_headers)
        self.assertIn('Value', df_headers)
        self.assertEqual(self.df_list[4]['CorrelationID'], "200")
        self.assertEqual(self.df_list[4]['NumberOfTrades'], 2)
        self.assertEqual(self.df_list[4]['Limit'], 2000)


    def test_final_df(self):
        self.final_df2 = self.obj.aggregate_df_corid()
        self.final_df = self.obj.write_to_csv()

        self.assertEqual(self.final_df["State"][self.final_df["CorrelationID"] == "200"].iloc[0],"Pending")
        self.assertEqual(self.final_df["State"][self.final_df["CorrelationID"] == "222"].iloc[0], "Rejected")
        self.assertEqual(self.final_df["State"][self.final_df["CorrelationID"] == "234"].iloc[0], "Accepted")

    def test_verify_csv(self):
        path = os.path.exists(self.result_csv)
        self.assertTrue(path)


