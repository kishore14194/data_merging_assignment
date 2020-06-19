# data_merging_assignment

##Assignment:

You are tasked to create script that will parse multiple csv's and create a unified csv. There are 3 different csv's, this will increase in the future. The client has as hard requirement that the result is stored as csv file. But json and xml will be used in the future, maybe even storing the result in a database.

In the data folder there are 3 csv's with banking data. Since the csv's come from different banks the layout of data can differ. The bank statements have data from the month October.

##Command:
User pass the path of the directory containing the csv file that needs to be merged using this command:

MergeCsv(r"</file path>").export_merged_files(file_output_path="</output file path>", file_name='</output_file_name>',output_format='</op format>')

Output exporting format can either be - csv, json, xml or sql. Exporting as Sql is yet to be implemented