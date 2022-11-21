from StatsSP500 import *
import unittest
import os
from tqdm import tqdm
import time

class TestSum(unittest.TestCase):

    def test_statsUpdate(self):
        firms = getFrims()
        mydb = mysql.connector.connect(
            host="127.01.01",
            user="root",
            password="123",
            database="SP500"
        )
        mycursor = mydb.cursor()
        
        for firmCode in tqdm(list(firms.keys())[80:]):
            result = updateSQLFirmStats(firmCode, mydb, mycursor)
            self.assertEqual(result, 0, f"{os.linesep}{result} on <{firmCode}>")

if __name__ == '__main__':
    unittest.main()