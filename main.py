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

def groupby_age_pdcd(arrests):
    """
       Counting arrests grouped by AGE_GROUP and PD_CD columns.

    Args:
        arrests (list of dict): List of dictionaries from csv arrest data.

    Returns:
        dict: Nested dictionary containing counts of arrests grouped by age group and PD_CD.
    """
    age_pd_arrests = defaultdict(int)
    for row in arrests:
        age_group = row['AGE_GROUP']
        pd_cd = row['PD_CD']
        if age_group not in age_pd_arrests:
            age_pd_arrests[age_group] = defaultdict(int)
        if pd_cd in age_pd_arrests[age_group]:
            age_pd_arrests[age_group][pd_cd] += 1
        else:
            age_pd_arrests[age_group][pd_cd] = 1
        
    return age_pd_arrests


if __name__ == '__main__':
    # Ingesting the data from csv file
    arrests = ingest('nypd-arrest-data-2018-1.csv')
    
    # Process and get the arrests by OFNS_DESC category in descending order
    ofns_desc_result = groupby_ofns_desc(arrests)
    
    # Slicing the top 10 counts and output it to console
    print("\nTop 10 Arrests based on OFNS_DESC: \n")
    for i, j in ofns_desc_result[:10]:
        print(f"{i}: {j}")

    age_pd_arrests = groupby_age_pdcd(arrests)

    # Retrieving the 4th greatest number of arrests grouped by PD_CD for each age group
    print("\n4th greatest number of arrests grouped by PD_CD for each AGE_GROUP\n")
    for age_group, pd_cd in age_pd_arrests.items():
        sorted_pd_cd = sorted(pd_cd.items(), key=lambda x: x[1], reverse=True)
        fourth_largest = sorted_pd_cd[3]
        print(f"Age Group: {age_group}, PD_CD: {fourth_largest[0]}, Arrests: {fourth_largest[1]}")

