import unittest
from main import ingest, groupby_ofns_desc

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
        result = ingest('nypd-arrest-data-2018-1.csv')[1:2]
        self.assertIsNotNone(result)

    def test_groupby_ofns_desc(self):
        # Test if data processing produces correct output
        result = groupby_ofns_desc(self.test_data)
        expected_result = [('ASSAULT', 3), ('ROBBERY', 1), ('BURGLARY', 1) ]
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()
