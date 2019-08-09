import pandas as pd
import xml.etree.ElementTree as et
import datetime


# log file
logfile = "Server.log"


class bnpTest():

    def __init__(self, input_file=None, corid=None):
        self.corid = corid
        self.pd_list = []
        self.input_file = input_file
        #self.logger = log_file(txt)

    def log_file(self, text):
        """ This is the loggin function. It writes the outcome to the server.log file"""
        with open(logfile, "a+") as text_file:
            text_file.write('\n' + datetime.datetime.now().strftime("%A, %d. %B %Y %I:%M%p") + '\n')
            text_file.write("==========================\n")
            text_file.write(text + "\n")

    def create_df_list(self, input_file):
        """ This Method is used to create a list based on the input file obtained
                The list will then be used to create a pandas DF object
                """
        self.input_file = input_file

        df_list = []

    # extract values from xml file using etree
        try:
            input_xml = self.input_file
            xml_root = et.parse(input_xml).getroot()

            for node in xml_root:
                df_dict = {}
                df_dict["CorrelationID"] = node.attrib.get("GPID")
                df_dict["NumberOfTrades"] = (int(node.find("NumberOfTrades").text))
                df_dict["Limit"] = (int(node.find("Limit").text))
                df_dict["Value"] = (int(node.find("Value").text))
                df_dict["TradeID"] = node.find("TradeID").text

                df_list.append(df_dict)
            return df_list

        except ValueError as e:
            txt = "The Value of this item should be an Integer"
            bnpTest.log_file(self, txt)

        except FileNotFoundError:
            txt = "The input xml file cant be found. Ensure its in the same directory\n"
            bnpTest.log_file(self, txt)

        except Exception as e:
            txt = f"{e}"
            bnpTest.log_file(self, txt)

    def create_df_series(self):
        """ This is the class method that creates the Pandas Data Frame."""
        df_cols = ["CorrelationID", "NumberOfTrades", "Limit", "Value", "TradeID"]
        df = pd.DataFrame(self.create_df_list("input.xml"), columns=df_cols)
        corid = [x for x in df.CorrelationID[::]]
        return df, corid

    def aggregate_df_corid(self):
        """ This is my class method that aggregates the data frame series. and prints the state of the orders"""
        df_order, corid = self.create_df_series()[0], self.create_df_series()[1]
        try:

            for corid in sorted(set(corid)):
                df_aggr = df_order[df_order.CorrelationID == corid]
                total_value = df_aggr['Value'].sum()
                new_df = df_aggr.iloc[0:1, 0:]
                new_df['Value'] = total_value
                if (df_aggr.NumberOfTrades > df_aggr.NumberOfTrades.count()).all():
                    new_df["State"] = "Pending"
                elif (new_df["Value"] > new_df["Limit"]).all():
                    new_df["State"] = "Rejected"
                elif (new_df["Value"] <= new_df["Limit"]).all():
                    new_df["State"] = "Accepted"

                filtered_df = new_df.drop(new_df.columns[2:5], 1)
                self.pd_list.append(filtered_df)
            #return self.pd_list
            bnpTest.write_to_csv(self)

        except Exception as e:
            txt = f"{e}"
            bnpTest.log_file(self, txt)

    def write_to_csv(self):
        try:
            final_df = pd.concat(self.pd_list)
            final_df.to_csv("results.csv", index=None)
            return final_df

        except ValueError as e:
            txt = f"Looks like something went wrong. Please check server logs file for more details. {e}" + "\n"
            bnpTest.log_file(self, txt)

        except Exception as e:
            txt = f"{e}"
            bnpTest.log_file(self, txt)
        finally:
            txt = "Requested result.csv file successfully written\n"
            bnpTest.log_file(self, txt)



