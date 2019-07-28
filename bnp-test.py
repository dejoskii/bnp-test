import pandas as pd
import xml.etree.ElementTree as et
import datetime
import sys

# log file

logfile = "Server.log"

with open(logfile, "a+") as text_file:
    text_file.write('\n' + datetime.datetime.now().strftime("%A, %d. %B %Y %I:%M%p") + '\n')
    text_file.write("==========================" + '\n')

df_list = []
df_cols = ["CorrelationID", "NumberOfTrades", "Limit", "Value", "TradeID"]

input_file = sys.argv[1]

try:
    xtree = et.parse(input_file)
    xroot = xtree.getroot()

    for node in xroot:
        df_dict = {}
        df_dict["CorrelationID"] = node.attrib.get("GPID")
        df_dict["NumberOfTrades"] = (int(node.find("NumberOfTrades").text))
        df_dict["Limit"] = (int(node.find("Limit").text))
        df_dict["Value"] = (int(node.find("Value").text))
        df_dict["TradeID"] = node.find("TradeID").text

        df_list.append(df_dict)
except ValueError as e:
    with open(logfile, "a+") as text_file:
        text_file.write("The Value of this item should be an Integer" + '\n')

except FileNotFoundError:
    with open(logfile, "a+") as text_file:
        text_file.write("The input xml file cant be found. Ensure its in the same directory" + '\n')


df = pd.DataFrame(df_list, columns = df_cols)
cid = [x for x in df.CorrelationID[::]]

pd_list=[]

def aggregate(corid):
    df_aggr = df[df.CorrelationID == corid]
    total_value = df_aggr['Value'].sum()
    new_df = df_aggr.iloc[0:1, 0:]
    new_df['Value'] = total_value

    if (new_df.NumberOfTrades.count() == 1 and new_df.NumberOfTrades == 2).all():
        new_df["State"] = "Pending"
    elif (new_df["Value"] > new_df["Limit"]).all():
        new_df["State"] = "Rejected"
    elif (new_df["Value"] < new_df["Limit"]).all():
        new_df["State"] = "Accepted"

    filtered_df = new_df.drop(new_df.columns[2:5], 1)
    pd_list.append(filtered_df)

try:
    for corid in sorted(set(cid)):
        new = aggregate(corid)

    final_df = pd.concat(pd_list)
    final_df.to_csv("results.csv", index=None)
    with open(logfile, "a+") as text_file:
        text_file.write("Requested result.csv file successfully written" + "\n" + "\n")
except ValueError as e:
    with open(logfile, "a+") as text_file:
        text_file.write(f"Looks like something went wrong. Please check server logs file for more details. {e}" + "\n")





