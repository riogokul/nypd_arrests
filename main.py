import csv
import sqlite3

def ingest(file):
    
    arrests = []
    with open(f'./{file}','r') as file:
        reader = csv.DictReader(file)
        for record in reader:
            arrests.append(record)
    return arrests


if __name__ == '__main__':
    # Ingesting the data from csv file
    arrests = ingest('nypd-arrest-data-2018-1.csv')
    