import unittest
import csv
import sqlite3

from main import ingest, groupby_ofns_desc, groupby_age_pdcd, to_csv,load_to_db

class TestArrestDataProcessing(unittest.TestCase):

    def setUp(self):
        # Set up test data for each test
        self.test_data = [
            {'ARREST_KEY': '184612066', 'PD_CD': '969', 'OFNS_DESC': 'ROBBERY', 'AGE_GROUP': '45-64'},
            {'ARREST_KEY': '184611183', 'PD_CD': '244', 'OFNS_DESC': 'BURGLARY', 'AGE_GROUP': '25-44'},
            {'ARREST_KEY': '184614200', 'PD_CD': '198', 'OFNS_DESC': 'ASSAULT', 'AGE_GROUP': '25-44'}, 
            {'ARREST_KEY': '184616006', 'PD_CD': '849', 'OFNS_DESC': 'ASSAULT', 'AGE_GROUP': '25-44'},
            {'ARREST_KEY': '184616008', 'PD_CD': '617', 'OFNS_DESC': 'ASSAULT', 'AGE_GROUP': '18-24'}
                        ]
    def test_ingest(self):
        #test whether csv file content is ingested

        result = ingest('nypd-arrest-data-2018-1.csv')[0]
        self.assertIsNotNone(result)

    def test_read_csv_file_valid(self):
        #checking for the total number of rows in the given file

        data = ingest('nypd-arrest-data-2018-1.csv')
        self.assertEqual(len(data), 131043)

    def test_groupby_ofns_desc(self):
        # Test if data processing produces correct output

        result = groupby_ofns_desc(self.test_data)
        expected_result = [('ASSAULT', 3), ('ROBBERY', 1), ('BURGLARY', 1) ]
        self.assertEqual(result, expected_result)

    def test_groupby_age_pdcd(self):
        # Test if counting arrests by age group and PD_CD is correct

        result = groupby_age_pdcd(self.test_data)
        expected_result = {
            '18-24': {'617': 1},
            '45-64': {'969': 1},
            '25-44': {'244': 1, '198': 1, '849': 1}
        }
        self.assertEqual(result, expected_result)

    def test_export_to_csv_file_written(self):
        #testing the csv files whether it was created and has the records in it

        data = ingest('nypd-arrest-data-2018-1.csv')
        keyword = 'ROBBERY'
        to_csv(data,keyword)

        with open(f'{keyword}_based_arrests.csv', 'r') as file:
            reader = csv.DictReader(file)
            
            first_row = next(reader)
            header_cols = ['ARREST_KEY', 'ARREST_DATE', 'AGE_GROUP', 'PD_CD']
            for i in header_cols:
                self.assertIn(i, first_row)
            
            #check whether the csv file contains any data
            second_row = next(reader)
            self.assertTrue(second_row)

    def test_load_to_db(self):
        #checking for created db and tables

        load_to_db(ingest('nypd-arrest-data-2018-1.csv'))

        conn = sqlite3.connect('nypd.db')
        cursor = conn.cursor()
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table";')
        tables = cursor.fetchall()
        first_row = cursor.execute('SELECT * FROM arrests limit 1;')
        conn.close()
        
        self.assertTrue(first_row)
        self.assertGreater(len(tables), 0)
        for i in tables:
            self.assertIn(i[0], 'arrests')

if __name__ == '__main__':
    unittest.main()
