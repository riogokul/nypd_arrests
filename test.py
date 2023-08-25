import unittest
from main import ingest

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

if __name__ == '__main__':
    unittest.main()
