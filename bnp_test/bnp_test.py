import xml.etree.ElementTree as et
import csv
import sys
import logging
import json

from collections import defaultdict, Counter

NO_TRADES_STRING = 'NumberOfTrades'


def setup_logging():
    # create logger with 'spam_application'
    logger = logging.getLogger('bnp_test')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('bnp_test.log')
    fh.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

class BnpTest():
    def __init__(self):
        self.logger = setup_logging()

    def read_xml(self, input_xml):
        try:
            root = et.parse(input_xml).getroot()
            data = []
            for child in root.iterfind("trade"):
                keys = []
                values = []
                for c in child:
                    keys.append(c.tag)
                    values.append(c.text)
                data.append(dict(zip(keys, values)))
            self.logger.info('Trades read from input file {} successfully'.format(input_xml))
            return data
        except:
            self.logger.error('Error reading trades from file {}'.format(input_xml))


    def aggregrate(self, dataset, group_by_key, sum_value_keys):
        try:
            dic = defaultdict(Counter)
            for item in dataset:
                key = item[group_by_key]
                vals = {k: int(item[k]) for k in sum_value_keys}
                dic[key][NO_TRADES_STRING] = int(item[NO_TRADES_STRING])
                dic[key]['limit'] = int(item['Limit'])
                dic[key]['count'] = dic[key].get('count', 0) + 1

                dic[key].update(vals)
            self.logger.info('Aggregated trades:{}:'.format(json.dumps(dic)))
            return dic

        except:
            self.logger.error('Error aggregating trades')

    def process(self, groups):
        try:
            output = []
            for k,v in groups.items():
                tmp = {'CorrelationID':k, NO_TRADES_STRING: v[NO_TRADES_STRING], 'Result': 'Rejected'}

                if v[NO_TRADES_STRING] != v['count']:
                    tmp['Result'] = 'Pending'

                elif v['Value'] <= v['limit']:
                    tmp['Result'] = 'Accepted'

                else:
                    tmp['Result'] = 'Rejected'
                output.append(tmp)
            self.logger.info('Processed trades:{}'.format(json.dumps(output)))
            return output
        except:
            self.logger.error('Error processing trades')

    def convert(self, input_xml):
        data = self.read_xml(input_xml)
        groups = self.aggregrate(data, 'CorrelationID', ['Value'])
        output = self.process(groups)
        sorted_output = sorted(output, key=lambda k: k['CorrelationID'])
        self.writeToCSV(sorted_output, 'results.csv')

    def writeToCSV(self,data, filename):
        try:
            self.logger.info('Writing processed result to file:{}'.format(filename))
            csv_header = data[0].keys()
            csv_file = filename
            with open(csv_file, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_header)
                writer.writeheader()
                writer.writerows(data)
            csvfile.close()
        except:
            self.logger.error('Error writing result to file {}'.format(filename))



bnpTest = BnpTest()
if len(sys.argv) < 2:
    print ('usage : python bnp_test.py input.xml ')
else:
    bnpTest.convert(sys.argv[1])