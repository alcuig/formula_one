from os import truncate

from numpy import single
from tools import read_data, get_race_id_from_data, get_set_race_ids
from operator import itemgetter

def winning_driver():
    """
    Creates list of dictionaries containing forename and surname of winning driver per race.
    Removes races where no winning driver present (due to missing data from source formula 1 .csv file).
    """
    data = read_data()
    # race_id_dataset = get_race_id_from_data(data)
    # ls_ids_race = []
    # for i in race_id_dataset:
    #     new_entry = i["race_id"]
    #     ls_ids_race.append(new_entry)
    # set_race_ids = set(ls_ids_race)
    set_race_ids = get_set_race_ids(data)

    ls_winning_driver_per_race = []

    #create a dataset which contains the rows of data pertinent to each race ("truncated dataset")
    ls_wining_driver_per_race = [] #list to store fastest laptime for each race

    single_race_winner_dataset = []
    for race in set_race_ids: 
        #code will cycle through data, creating temporary dataset for each race
        #this is where the temporary data set starts to get populated
        for line in data:
            if line["raceId"] == race:
                line = {
                    "race_id" : line["raceId"],
                    "winning_driver_surname" : line["driver_surname"],
                    "winning_driver_forename" : line["driver_forename"],
                    "final_position" : line["final_position"]
                }
                single_race_winner_dataset.append(line) 

    for line in single_race_winner_dataset:
        if (line["final_position"]) == "1":
            ls_winning_driver_per_race.append(line)

    winning_driver_data = ls_winning_driver_per_race
    return(winning_driver_data)
