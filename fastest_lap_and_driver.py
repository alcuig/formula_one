from os import truncate
from tools import read_data, get_race_id_from_data, get_set_race_ids
from operator import itemgetter
import sys

def convert_time_to_seconds(time):
    """
    Converts time input in a 'XX:XX.X' string format, into seconds, in an integer format. 

    This then allows you to compare laptimes to determine who was the fastest or slowest driver per race.
    
    """
    y = (f"0.{int(time[-1])}")
    lap_time_int = int(time[0:2]) * 60 + int(time[3:5]) + float(y)
    return lap_time_int

def fastest_lap_and_driver():
    """
    Returns fastest_lap_and_driver_data : a list of dictionaries containing the forename and surname of the driver who performed the fastest lap per race, 
    as well as the time of the fastest lap in seconds.

    Uses sorted function to sort list of laptimes, from smallest (fastest) to largest (slowest).
    List[0] to obtain value at position 0 (a.k.a the smallest value, and therefore fastest laptime).

    Removes from final dataset the races where no fastest lap was performed, as a result of missing data from source .csv file.

    Bug fixed : "convert_time_to_seconds" : to compare the laptimes to determine who performed the fastest lap in the race, 
    time in regular expression had to be converted to an int (seconds).

    """
    data = read_data()
    set_race_ids = get_set_race_ids(data)
    #create a dataset which contains the rows of data pertinent to each race ("truncated dataset")
    # ls_fastest_laptime_per_race = [] #list to store fastest laptime for each race
    ls_fastest_laps_and_drivers_per_race = []
    for race in set_race_ids: 
        single_race_dataset = []
        single_race_laptimes_in_seconds = []
        
        #code will cycle through data, creating temporary dataset for each race
        #this is where the temporary data set starts to get populated
        for line in data:
            if line["raceId"] == race:
                line = {
                    "race_id" : line["raceId"],
                    "fastest_lap_driver_surname" : line["driver_surname"],
                    "fastest_lap_driver_forename" : line["driver_forename"],
                    "fastest_lap_time" : line["fastestLapTime"]
                }
                single_race_dataset.append(line) 

        for row in single_race_dataset:
            if len(row["fastest_lap_time"].strip()) == 7: #to filter out the Null (/N) laptime values in the table
                row["fastest_lap_time"] = convert_time_to_seconds(row["fastest_lap_time"])
            else:
                row["fastest_lap_time"] = None
                
        
        single_race_dataset_without_nones = []
        for row in single_race_dataset:
            if row["fastest_lap_time"] is not None:
                single_race_dataset_without_nones.append(row)
            
        
        single_race_dataset = single_race_dataset_without_nones 

        sorted_by_fastest_laptime_ls = sorted(single_race_dataset, key=itemgetter('fastest_lap_time'))
        if len(sorted_by_fastest_laptime_ls) != 0: 
            fastest_driver = sorted_by_fastest_laptime_ls[0]
            ls_fastest_laps_and_drivers_per_race.append(fastest_driver)

    fastest_lap_and_driver_data = ls_fastest_laps_and_drivers_per_race
    return fastest_lap_and_driver_data
