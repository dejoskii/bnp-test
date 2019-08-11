import pandas as pd
import xml.etree.ElementTree as et
import datetime
import sys

class BnpTest():

    def __init__(self,corid=None):
        self.corid = corid
        self.pd_list = []
        self.df = []
        self.corid = []
        self.df_list = []
        self.input_file = "input.xml"
        self.logfile = "Server.log"
        self.output_file = "results.csv"

    def log_file(self, text):
        """ This is the loggin function. It writes the outcome to the server.log file"""
        with open(self.logfile, "a+") as text_file:
            text_file.write('\n' + datetime.datetime.now().strftime("%A, %d. %B %Y %I:%M%p") + '\n')
            text_file.write("==========================\n")
            text_file.write(text + "\n")

    def create_df_list(self,input_file):
        """ This Method is used to create a list based on the input file obtained
                The list will then be used to create a pandas DF object
                """
        self.input_file = input_file

    # extract values from xml file using etree
        try:
            xml_root = et.parse(self.input_file).getroot()
            for item in xml_root:
                df_dict = {}
                for ele in item:
                    df_dict[ele.tag] = ele.text
                self.df_list.append(df_dict)
            return self.df_list

        except ValueError as e:
            txt = "The Value of this item should be an Integer"
            BnpTest.log_file(self, txt)
        except FileNotFoundError:
            txt = "The input xml file cant be found. Ensure its in the same directory\n"
            BnpTest.log_file(self, txt)
        except Exception as e:
            txt = f"{e}"
            BnpTest.log_file(self, txt)

    def create_df_series(self):
        """ This is the class method that creates the Pandas Data Frame. The df.apply(pd.to_numeric, converts
        the relevant series in the data frame to integers"""

        df_cols = ["CorrelationID", "NumberOfTrades", "Limit", "Value", "TradeID"]
        df = pd.DataFrame(self.df_list, columns=df_cols)
        df_cols_drop = df.columns.drop(['CorrelationID', 'TradeID'])  # drop columns that are strings
        df[df_cols_drop] = df[df_cols_drop].apply(pd.to_numeric, errors="coerce")
        self.df.append(df)
        corid = [x for x in df.CorrelationID[::]]
        self.corid.append(corid)
        return corid, df

    def aggregate_df_corid(self):
        """ This is my class method that aggregates the data frame series. and prints the state of the orders"""
        df_order, corid = self.df[0], self.corid[0]

        try:
            for corid in sorted(set(corid)):
                df_aggr = df_order[df_order.CorrelationID == corid]
                total_value = df_aggr["Value"].sum()
                new_df = df_aggr.iloc[0:1, 0:]  # get the first row in the series
                new_df["Value"] = total_value
                if (df_aggr.NumberOfTrades > df_aggr.NumberOfTrades.count()).all():
                    new_df["State"] = "Pending"
                elif (new_df["Value"] > new_df["Limit"]).all():
                    new_df["State"] = "Rejected"
                elif (new_df["Value"] <= new_df["Limit"]).all():
                    new_df["State"] = "Accepted"
                filtered_df = new_df.drop(new_df.columns[2:5], 1)
                self.pd_list.append(filtered_df) # Add each filtered DF to the empty list
            return self.pd_list

        except Exception as e:
            txt = f"{e}"
            BnpTest.log_file(self, txt)

    def write_to_csv(self, output_file):
        self.output_file = output_file
        try:
            final_df = pd.concat(self.pd_list, ignore_index=True) # Add all DF in the list to form one single DF
            final_df.to_csv(self.output_file, index=None)
            return final_df

        except ValueError as e:
            txt = f"Looks like something went wrong. Please check server logs file for more details. {e}" + "\n"
            BnpTest.log_file(self, txt)

        except Exception as e:
            txt = f"{e}"
            BnpTest.log_file(self, txt)
        finally:
            txt = "Requested result.csv file successfully written\n"
            BnpTest.log_file(self, txt)

    def call_all(self):
        bnp_orders.create_df_list(sys.argv[1])
        bnp_orders.create_df_series()
        bnp_orders.aggregate_df_corid()
        bnp_orders.write_to_csv("results.csv")


if len(sys.argv) < 2:
    print('usage : python bnp_test.py input.xml ')
else:
    bnp_orders = BnpTest()
    bnp_orders.call_all()
