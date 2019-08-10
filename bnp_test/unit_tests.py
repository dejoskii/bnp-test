import unittest
import os
from bnp import BnpTest

class BnpUnitTests(unittest.TestCase):
    data = {}
    correlation_id = None
    def setUp(self):
        self.obj = BnpTest()
        self.xml_file = 'input.xml'
        self.result_csv = 'results.csv'

    def test_a_read_xml(self):
        response = self.obj.read_xml(self.xml_file)
        keys = response[0].keys()
        self.assertIn('CorrelationID', keys)
        self.assertIn('NumberOfTrades', keys)
        self.assertIn('Value', keys)
        self.assertEqual(response[0]['CorrelationID'], '234')
        self.assertEqual(response[0]['NumberOfTrades'], '3')
        self.assertEqual(response[0]['Limit'], '1000')


    def test_b_aggregate(self):
        data = [
            {'CorrelationID': '234', 'NumberOfTrades': '3', 'Limit': '1000', 'Value': '100', 'TradeID': '654'},
            {'CorrelationID': '234', 'NumberOfTrades': '3', 'Limit': '1000', 'Value': '200', 'TradeID': '135'},
            {'CorrelationID': '222', 'NumberOfTrades': '1', 'Limit': '500', 'Value': '600', 'TradeID': '423'},
            {'CorrelationID': '234', 'NumberOfTrades': '3', 'Limit': '1000', 'Value': '200', 'TradeID': '652'},
            {'CorrelationID': '200', 'NumberOfTrades': '2', 'Limit': '2000', 'Value': '1000', 'TradeID': '645'}
        ]
        response = self.obj.aggregrate(data, 'CorrelationID', ['Value'])
        self.assertIn('count', response['234'])
        self.assertEqual(response['234']['count'], 3)
        self.assertEqual(response['222']['count'], 1)
        self.assertEqual(response['200']['count'], 1)


    def test_c_process(self):
        aggregated_data = {
            '234': {'limit': 1000, 'Value': 500, 'NumberOfTrades': 3, 'count': 3},
            '222': {'Value': 600, 'limit': 500, 'NumberOfTrades': 1, 'count': 1},
            '200': {'limit': 2000, 'Value': 1000, 'NumberOfTrades': 2, 'count': 1}
        }
        response = self.obj.process(aggregated_data)

        expected_result = [
            {'CorrelationID': '234', 'NumberOfTrades': 3, 'Result': 'Accepted'},
            {'CorrelationID': '222', 'NumberOfTrades': 1, 'Result': 'Rejected'},
            {'CorrelationID': '200', 'NumberOfTrades': 2, 'Result': 'Pending'}
        ]
        self.assertListEqual(response, expected_result)

    def test_d_writeToCSV(self):
        data_to_write = [
            {'CorrelationID': '234', 'NumberOfTrades': 3, 'Result': 'Accepted'},
            {'CorrelationID': '222', 'NumberOfTrades': 1, 'Result': 'Rejected'},
            {'CorrelationID': '200', 'NumberOfTrades': 2, 'Result': 'Pending'}
        ]
        self.obj.writeToCSV(data_to_write, "unit_test_output.csv")
        e = os.path.exists("unit_test_output.csv")
        self.assertTrue(e)
        #     TODO - this test and assertions can be improvved given more time

    def test_convert(self):
        self.obj.convert(self.xml_file)
        e = os.path.exists("results.csv")
        self.assertTrue(e)
        #     TODO - this test and assertions can be improvved given more time

if __name__ == "__main__":
    unittest.main()
