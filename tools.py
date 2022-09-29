import csv
import pprint

def read_data():
    """
    Reads data from source .csv file for formula 1 data ("drivers.csv") and converts into a list of dictionaries.
    """
    header = []
    rows = []
    with open("drivers.csv", "r") as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            rows.append(row)

    data = []
    for row in rows:
        line = {}
        for i in range(len(header)):
            line[header[i]] = row[i]
        data.append(line)
    return data

def get_drivers_from_data(data):
    "Creates list of dictionaries containing unique driver names."
    driver_names = []
    for line in data:
        new_entry = {
            "driver_forename": line["driver_forename"],
            "driver_surname": line["driver_surname"],
            "driver_nationality": line["driver_nationality"],
            "driver_dob": line["driver_dob"]
        }
        if new_entry not in driver_names:
            driver_names.append(new_entry)
    return driver_names

def get_race_id_from_data(data):
    """
    Creates a list of unique race ids, a 'set'.
    The function will remove the duplicate race ids from the orginal drivers table dataset 
    (which result from multiple drivers participating in a race, see below).
    
    Race                      Driver
    race_1                    Lewis
    race_1 #duplicate         Max
    race_1 #duplicate         George
    race_2                    Lewis
    race_2 #duplicate         Max
    etc.)

    returns something like: ls = [{'race_id': '894'}, {'race_id': '895'}]

    """
    race_id = []
    for line in data:
        new_entry = {"race_id": line["raceId"]}
        if new_entry not in race_id:
            race_id.append(new_entry)
    return race_id

def get_set_race_ids(data):
    """
    Returns something like [1,2,3,4]
    """
    race_ids = get_race_id_from_data(data)

    set_race_ids = []
    for race in race_ids:
        if race['race_id'] not in set_race_ids:
            set_race_ids.append(race['race_id'])
    return set_race_ids













