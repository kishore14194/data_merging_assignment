import os
import re
import pandas as pd

SUPPORTED_FORMAT = ['csv', 'xml', 'json', 'sql']


class FormatError(Exception):
    """Exception raised when mentioned data type is not valid"""


class OutputFormatError(Exception):
    """Exception raised when output format is not valid"""


class MergeCsv():
    def __init__(self, file_path):
        """
        :param file_path {string}: Path of the file that contains the csv to merge
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError("Please specify a correct folder path")
        self.bank_file_data = self.get_files(file_path)

    def merge(self):
        """
        Merges all the csv file into a single dataframe
        :return: returns the merged dataframe
        """
        default_header_format = self.default_csv_data_format()
        formatted_bank_csv_data_list = []
        for bank in self.bank_file_data:
            # Format csv data uniformly
            format_csv = self.format_csv_data(self.bank_file_data[bank], default_header_format)
            formatted_bank_csv_data_list.append(format_csv)
        merged_dataframe = pd.concat(formatted_bank_csv_data_list, sort=False).reset_index(drop=True)

        return merged_dataframe

    def get_files(self, file_path):
        """
        Function to get all the csv file under the folder and reads the csv content
        :param file_path {string}: Folder/file path of the directory
        :return: dictionary with csv data
        """
        if file_path:
            os.chdir(file_path)
        file_name_list = os.listdir(file_path)
        csv_pattern = re.compile(r'.*\.csv')
        csv_dict = {}
        for name in file_name_list:
            new_name = re.findall(csv_pattern, name)
            if len(new_name) != 0:
                csv_dict[new_name[0].replace('.csv', '')] = pd.read_csv(name)

        return csv_dict

    def default_csv_data_format(self):
        """
        Assigned a dictionary to map different headers in different csv's
        :return: Dictionary with default fomat
        """
        default_data_dict = {
            'date': (['timestamp', 'date', 'date_readable'], 'datetime'),
            'type': (['type', 'transaction'], 'string'),
            'amount': (['amount', 'amounts'], 'float'),
            'to': (['to'], 'integer'),
            'from': (['from'], 'integer')
        }

        return default_data_dict

    def preprocess_dataframe(self, csv_dataframe):
        # TODO: Make preprocessing configurable
        # Preprocess euros and cents into amount.
        if 'euro' and 'cents' in csv_dataframe:
            csv_dataframe['amount'] = csv_dataframe['euro'] + (
                    csv_dataframe['cents'] / 10 ** len(csv_dataframe['cents']))
            csv_dataframe.drop(columns=['euro', 'cents'], inplace=True)

        return csv_dataframe

    def export_merged_files(self, file_output_path=None, file_name='merged_csv_data', output_format='csv'):
        """
        Export the merged file into desired format
        :param file_output_path {string}: Output file path
        :param file_name {string}:Name of the file
        :param output_format {string}: out format limited to csv,xml,json and sql
        :return:
        """

        if output_format not in SUPPORTED_FORMAT:
            raise OutputFormatError("{} is not supported. Please use one of the following - {}".format(
                output_format, SUPPORTED_FORMAT))

        if not file_output_path:
            file_output_path = os.getcwd()
        merged_data = self.merge()
        output_file = "{}/{}.{}".format(file_output_path, file_name, output_format)
        if output_format == 'csv':
            merged_data.to_csv(output_file, index=False)
        if output_format == 'json':
            merged_data.to_json(output_file)
        if output_format == 'xml':
            xml_data = self.export_to_xml(merged_data)
            with open(output_file, 'w') as f:
                f.write(xml_data)
        if output_format == 'sql':
            return "This feature will be available later"

        return "Files merged successfully"

    def export_to_xml(self, dataframe):
        def row_to_xml(row):
            xml = ['<item>']
            for i, col_name in enumerate(row.index):
                xml.append('  <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
            xml.append('</item>')
            return '\n'.join(xml)

        res = '\n'.join(dataframe.apply(row_to_xml, axis=1))
        return res

    def format_csv_data(self, csv_data, default_col):
        """
        Format the csv data and convert it into formatted dataframe
        :param csv_data <class:dataframe>: csv dataframe
        :param default_col: default column formatting dictionary
        :return:
        """
        csv_dataframe = csv_data.copy()
        csv_dataframe = self.preprocess_dataframe(csv_dataframe)

        for key, value in default_col.items():
            for col in csv_dataframe.columns:
                if col in value[0]:
                    csv_dataframe.rename(columns={col: key}, inplace=True)
                    try:
                        if value[-1] == 'datetime':
                            csv_dataframe['date'] = pd.to_datetime(csv_dataframe[key], dayfirst=True)
                        if value[-1] == 'string':
                            csv_dataframe[key] = csv_dataframe[key].astype('str')
                        if value[-1] == 'float':
                            csv_dataframe[key] = csv_dataframe[key].astype('float')
                        if value[-1] == 'integer':
                            csv_dataframe[key] = csv_dataframe[key].astype('int')
                    except FormatError():
                        print("Column {} cannot be converted to requested format".format(key))

        return csv_dataframe


MergeCsv(r"/Users/kishore/Documents/data_merging_assignment/Sobers_Assignment/sample_input_files").export_merged_files(
    output_format='xml')
