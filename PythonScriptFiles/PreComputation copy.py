import json
import csv
import numpy as np
import math
import urllib
from urllib.request import urlopen
from urllib.request import Request

header_row=['source_coords','dest1_coords','dest2_coords','pickup_time','passenger_count','ret_angle','source_D1_distance(in miles)','source_D1_time(in minutes)' ,'source_D1_avg_speed(per minute)']
header_row.extend(['original_cost$2','original_cost$2.5','original_cost$3','original_cost$4','original_accepted_delay'])
header_row.extend(['D1_D2_distance(in miles)','D1_D2_time(in minutes)' ,'D1_D2_avg_speed(per minute)','D1_D2_cost$2','D1_D2_cost$2.5','D1_D2_cost$3','D1_D2_cost$4'])

unique_dest=set()
global_pair_sets=set()
trip_dict={}
passenger_dict={}
source_coords= (-73.77685547,40.64508438)


def create_unique_dest_list():
    with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/taxi1000.csv', 'r') as csvreaderfile:
    ##with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/taxi1000.csv', 'r') as csvreaderfile:
        reader = csv.DictReader(csvreaderfile)
        unique_dest=set() 
        for row in reader:
            timestamp=row["tpep_pickup_datetime"]
            passenger_count=row["passenger_count"]
            dest_coords = (float(row["dropoff_latitude"]) , float (row["dropoff_longitude"]) )
            unique_dest.add(dest_coords)

            if dest_coords not in trip_dict:    
                trip_dict[dest_coords]=[timestamp]
            if dest_coords not in passenger_dict:
                passenger_dict[dest_coords]=[passenger_count]

        return unique_dest

    
def main():
    unique_dest=create_unique_dest_list()
    
    print("****************************************")
    print("Length of Unique Destinations in File: ", len(unique_dest))
    print("****************************************")
    import operator
    sorted_x = sorted(trip_dict.items(), key=operator.itemgetter(1))
    for x,v in sorted_x:
        print(v)

    




if __name__ == '__main__':
    main()
    
