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


def to_csv(arrests,ofns):
    """
        Filter the arrests data based on the given input of OFNS_DESC and export it to a csv file.
        
    Args:
        arrests (list of dict): List of dictionaries from csv arrest data.
        ofns (str): Full or partial user input on OFNS_DESC.

    Returns:
        None (Saves a csv file in the source path)
    """

    filtered_data = [row for row in arrests if ofns.lower() in row['OFNS_DESC'].lower()]

    with open(f"{ofns}_based_arrests.csv", 'w', newline='') as f:
        csv_writer = csv.DictWriter(f, fieldnames=arrests[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(filtered_data)


def load_to_db(arrests):
    """
        Create an SQLite database, create a table 'arrests' and insert all data into it.

    Args:
        arrests (list of dict): List of dictionaries from csv arrest data.

    Returns:
        None (Data written to db)
    """

    conn = sqlite3.connect('nypd.db')
    cursor = conn.cursor()

    # Creating a table
    cursor.execute('''CREATE TABLE IF NOT EXISTS arrests
                      ( ARREST_KEY INTEGER PRIMARY KEY, 
                        ARREST_DATE TEXT, 
                        PD_CD INTEGER,
                        PD_DESC TEXT,
                        KY_CD INTEGER,
                        OFNS_DESC TEXT, 
                        LAW_CODE TEXT,
                        LAW_CAT_CD TEXT,
                        ARREST_BORO TEXT,
                        ARREST_PRECINCT INTEGER,
                        JURISDICTION_CODE INTEGER,
                        AGE_GROUP TEXT,
                        PERP_SEX TEXT,
                        PERP_RACE TEXT,
                        X_COORD_CD INTEGER,
                        Y_COORD_CD INTEGER,
                        Latitude REAL,
                        Longitude REAL)''')
    
    # Insert data into the table
    try:
        for row in arrests:
            cursor.execute("INSERT INTO arrests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (row['ARREST_KEY'],row['ARREST_DATE'],row['PD_CD'],row['PD_DESC'],row['KY_CD'],row['OFNS_DESC'],row['LAW_CODE'],row['LAW_CAT_CD'],row['ARREST_BORO'],row['ARREST_PRECINCT'],row['JURISDICTION_CODE'],row['AGE_GROUP'],row['PERP_SEX'],row['PERP_RACE'],row['X_COORD_CD'],row['Y_COORD_CD'],row['Latitude'],row['Longitude']
                            ))
    except Exception as e:
        print (f"\nException occured {e} \nOnly unique values for ARREST_KEY can be inserted")
    finally:
        print("All recorded loaded to the db")
    
    conn.commit()
    conn.close()

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


    # Export data to CSV based on input vaalue for OFNS_DESC
    ofns = input("\nEnter the value for OFNS_DESC:\n ")
    to_csv(arrests, ofns)
    
    # Creating SQLite database and inserting data
    load_to_db(arrests)
    