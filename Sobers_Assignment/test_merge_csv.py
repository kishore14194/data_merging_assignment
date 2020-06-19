import unittest
import csv
import os
from merge_csv import MergeCsv


def read_data(data):
    with open(data, 'r') as f:
        data = [row for row in csv.reader(f.read().splitlines())]
    return data


class ParseCSVTest(unittest.TestCase):

    def setUp(self):
        output_path = r"/Users/kishore/Documents/data_merging_assignment/Sobers_Assignment/sample_input_files"
        op_file_name = "merged_test_csv_data"
        MergeCsv(r"/Users/kishore/Documents/data_merging_assignment/Sobers_Assignment/sample_input_files").\
            export_merged_files(file_output_path=output_path, file_name='merged_test_csv_data')
        self.data = "{}/{}.{}".format(output_path, op_file_name, 'csv')

    def test_csv_read_data_headers(self):
        self.assertEqual(
            read_data(self.data)[0],
            ["date", "type", "amount", "from", "to"])

    def test_csv_read_data_date(self):
        self.assertEqual(read_data(self.data)[1][0], '2019-10-01')

    def test_csv_read_transaction_type(self):
        self.assertEqual(read_data(self.data)[1][1], 'remove')

    def tearDown(self):
        os.remove(self.data)


if __name__ == '__main__':
    unittest.main()
