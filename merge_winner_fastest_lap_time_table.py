
from os import setsid
from fastest_lap_and_driver import fastest_lap_and_driver
from winning_driver import winning_driver
from tools import get_set_race_ids, read_data
import sys

def merged_winner_fastest_lap_records():
    """
    Returns list of dictionaries 'merged_datasets' combining data from dataset containign fastest laptime and driver who performed fastest laptime with dataset containing name of winning dirver per race.

    Achieves this by:
    - Cycling through race id set 
    - Then through  fastest_lap_and_driver_data or winning_driver_data sets 
    - Comparing race ids
    - By default it will add 'no data available' as value for each key
    - But if the for loop finds a match 
    -It adds the relevant columns as key:value pairs in an empty 'object' dictionary
    - Which it will append to the new merged_datasets list once it has cycled through both datasets
    """
    fastest_lap_and_driver_data = fastest_lap_and_driver()
    winning_driver_data = winning_driver()
    data = read_data()
    set_of_race_ids = get_set_race_ids(data)
    merged_datasets = []
    
    for race in set_of_race_ids:
        obj = {'race_id': race}
        for row in fastest_lap_and_driver_data:
            x = row["race_id"]
            if int(x) == int(race):
                obj['fastest_lap_time'] = row['fastest_lap_time']
                obj['fastest_lap_driver_forename'] = row['fastest_lap_driver_forename']
                obj['fastest_lap_driver_surname'] = row['fastest_lap_driver_surname']
                break
            
            obj['fastest_lap_time'] = "No data available"
            obj['fastest_lap_driver_forename'] = "No data available"
            obj['fastest_lap_driver_surname'] = "No data available"
            
        for row in winning_driver_data:
            x = row["race_id"]
            if int(x) == int(race):
                obj['winning_driver_forename'] = row['winning_driver_forename']
                obj['winning_driver_surname'] = row['winning_driver_surname']
                break
            
            obj['winning_driver_forename'] = "No data available"
            obj['winning_driver_surname'] = "No data available"
    
        merged_datasets.append(obj)
        
    return merged_datasets
