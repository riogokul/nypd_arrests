import csv
import sqlite3

from collections import defaultdict

def ingest(file):
    """
        To ingest the csv file to a list of dict

    Args:
        file (str): filename of the csv

    Returns:
         List of dictionaries from csv arrest data.
    """
    arrests = []
    with open(f'./{file}','r') as file:
        reader = csv.DictReader(file)
        for record in reader:
            arrests.append(record)
    return arrests

def groupby_ofns_desc(arrests):
    """
        Manipluate the provided data by counting arrests grouped by OFNS_DESC.

    Args:
        arrests (list of dict): List of dictionaries from csv arrest data.

    Returns:
        dict: Dictionary with counts of arrests grouped by OFNS_DESC.
    """
    count = defaultdict(int)
    for row in arrests:
        key = row['OFNS_DESC']
        if key in count:
            count[key] += 1
        else:
            count[key] = 1

    final_count = list(sorted(count.items(),reverse=True, key=lambda x:x[1]))

    return (final_count)


if __name__ == '__main__':
    # Ingesting the data from csv file
    arrests = ingest('nypd-arrest-data-2018-1.csv')
    
    # Process and get the arrests by OFNS_DESC category in descending order
    ofns_desc_result = groupby_ofns_desc(arrests)
    
    # Slicing the top 10 counts and output it to console
    print("\nTop 10 Arrests based on OFNS_DESC: \n")
    for i, j in ofns_desc_result[:10]:
        print(f"{i}: {j}")

 