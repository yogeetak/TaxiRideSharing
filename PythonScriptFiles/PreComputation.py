import json
import csv
import sys
import numpy as np
import math
import urllib
from urllib.request import urlopen
from urllib.request import Request
import operator
from datetime import datetime,timedelta

header_row=['source_coords','dest1_coords','dest2_coords','pickup_time','passenger_count','ret_angle','source_D1_distance(in miles)','source_D1_time(in minutes)' ,'source_D1_avg_speed(per minute)']
header_row.extend(['original_cost$2','original_cost$2.5','original_cost$3','original_cost$4','original_accepted_delay','new_50_accepted_delay'])
header_row.extend(['D1_D2_distance(in miles)','D1_D2_time(in minutes)' ,'D1_D2_avg_speed(per minute)','D1_D2_cost$2','D1_D2_cost$2.5','D1_D2_cost$3','D1_D2_cost$4'])

unique_dest=set()
trip_dict={}
passenger_dict={}
source_coords= (-73.785924,40.645134)


def create_unique_dest_list():
    
    ##with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/TaxiData-20000-1.csv', 'r') as csvreaderfile:
    ##with open('C:/Users/pravaljain/PycharmProjects/TaxiRideSharing/Taxi Cleaned Data/TaxiData-20000-1.csv', 'r') as csvreaderfile:
    with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/January_Week_1.csv', 'r') as csvreaderfile:
        reader = csv.DictReader(csvreaderfile)
        unique_dest=set() 
        for row in reader:
            try:
                dest_coords = (float(row["dropoff_longitude"]) , float (row["dropoff_latitude"]) )
                timestamp=row["tpep_pickup_datetime"]
                try: ##Handling dates of format Year: 16, 2016
                    datetime_timestamp = datetime.strptime(timestamp, '%m/%d/%y %H:%M')
                except ValueError:
                    datetime_timestamp = datetime.strptime(timestamp, '%m/%d/%Y %H:%M')
                               
                passenger_count=row["passenger_count"]
                unique_dest.add(dest_coords)
                if dest_coords not in trip_dict:    
                    trip_dict[dest_coords]=datetime_timestamp
                    
                if dest_coords not in passenger_dict:
                    passenger_dict[dest_coords]=passenger_count
            except Exception as e:
                print("Exception in retriving following points from source: ", dest_coords,e)
                continue
        return unique_dest

            
def latlong_to_3d(latr, lonr):
    """Convert a point given latitude and longitude in radians to
    3-dimensional space, assuming a sphere radius of one."""
    return np.array((
        math.cos(latr) * math.cos(lonr),
        math.cos(latr) * math.sin(lonr),
        math.sin(latr)
    ))

def angle_between_vectors_degrees(u, v):
    """Return the angle between two vectors in any dimension space,
    in degrees."""
    return np.degrees(
        math.acos(np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v))))

def cal_angle(A, B, C):

    try:
        angle3deg= -1

        # Convert the points to numpy latitude/longitude radians space
        a = np.radians(np.array(A))
        b = np.radians(np.array(B))
        c = np.radians(np.array(C))

        # The points in 3D space
        a3 = latlong_to_3d(*a)
        b3 = latlong_to_3d(*b)
        c3 = latlong_to_3d(*c)

        # Vectors in 3D space
        a3vec = a3 - b3
        c3vec = c3 - b3

        # Find the angle between the vectors in 2D space
        angle3deg = angle_between_vectors_degrees(a3vec, c3vec)

    except:
        print("Exception in calculating angel for following points: ", A,B,C)
        angle3deg= -1
        return angle3deg

    return angle3deg

def osrm_distance_cal(point1, point2):
    try:      
        d1_latitude,d1_longitude=point1
        d2_latitude,d2_longitude=point2

        url = "http://127.0.0.1:5000/route/v1/driving/"+str(d1_latitude) +","+str(d1_longitude)+";" +str(d2_latitude) +","+str(d2_longitude)
        response = urlopen(url)
        string = response.read().decode('utf-8')
        json_obj = json.loads(string)
        
        # trip distance in miles
        trip_distance = json_obj['routes'][0]['distance'] * float(0.000621371)

        # trip duration in minutes
        trip_duration = json_obj['routes'][0]['duration'] * float(0.0166667)

        # average_speed in miles per minute
        average_speed = float(trip_distance)/float(trip_duration)

    except TypeError as e: 
        print('Error: ', e)
        return -1,-1,-1
    except Exception as e: 
        print('Exception in calculating OSRM distances',point1, point2, e)
        return -1,-1,-1
            
    return round(trip_distance,2),round(trip_duration,2),round(average_speed,2)

    
def main():
    try:
        
        global global_pair_sets
        unique_dest=create_unique_dest_list()
        temp_trip_dict=trip_dict  ##Values will be deleted from the temp dictionary to avoid recalculating

        print("****************************************")
        print("Length of Unique Destinations in File: ", len(unique_dest))
        print("****************************************")
        print()

        ##Opening csv file to write pre computed data
        ##with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/PreComputed_TaxiData-20000-1.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
        ##with open('C:/Users/pravaljain/PycharmProjects/TaxiRideSharing/Taxi Cleaned Data/PreComputed_TaxiData-20000-1.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
        with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/PreComputed_January_Week_1.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
            writer = csv.writer(csvwriterfile, dialect='excel')
            writer.writerow(header_row)

            ##Sorting the trips by PickUp Times in ascending order:
            sorted_x = sorted(trip_dict.items(), key=operator.itemgetter(1))
            key,starttime = sorted_x[0]
            lastindex=len(sorted_x) - 1
            key,endtime= sorted_x[lastindex]

            print("****************************************")
            print("Start Time of File", starttime)
            print("End Time of File", endtime)
            print("****************************************")
            print()

            ##Create Time intervals of 5 mins
            range_q=starttime - timedelta(minutes=5)
            while( range_q < endtime):
                range_q =range_q + timedelta(minutes=5)
                
                matchings=0
                ##Retrieve from dictionary only rows pertaining to current time period
                time_interval_keys = [x for x in temp_trip_dict if temp_trip_dict[x] <= range_q]
                if len(time_interval_keys) == 0:
                    continue
                
                print("Current Time Window: ",range_q)
                print("Destination Group Count", len(time_interval_keys))
                
                ##Iterate Values only for the time periods, match within same time period
                for dest_1 in time_interval_keys:
                    ##Calculate Distance, Time & Average Speed from (S, D1)
                    source_D1_distance ,source_D1_time ,source_D1_avg_speed  = osrm_distance_cal(source_coords,dest_1)
                    if source_D1_distance == -1 or source_D1_time == -1 or source_D1_avg_speed == -1:
                        continue
                    
                    ##Calculating Acceptable Delay
                    if source_D1_distance >= 5:
                        original_accepted_delay = (source_D1_time * 30)/100
                    else:
                        original_accepted_delay = (source_D1_time * 50)/100

                    ##New acceptable delay
                    new_50_accepted_delay = (source_D1_time * 50)/100
                        
                        
                    for dest_2 in time_interval_keys:
                         
                        ##Dont calculate for same destinations  - (S,D1,D1), But write values to file
                        if(dest_1 == dest_2):
                            ##Writing all pre computed values to csv file
                            temp_row=[source_coords,dest_1,' ',trip_dict[dest_1],passenger_dict[dest_1],'',source_D1_distance ,source_D1_time ,source_D1_avg_speed]
                            temp_row.extend([source_D1_distance*2,source_D1_distance*2.5,source_D1_distance*3,source_D1_distance*4,original_accepted_delay,new_50_accepted_delay])    
                            temp_row.extend(['' ,'' ,'','','','',''])
                            writer.writerow(temp_row)
                            continue

                        ##Calculate Angles of datapoints (S,D1,D2) - The points in tuple latitude/longitude degrees space
                        ret_angle = cal_angle(source_coords,dest_1,dest_2)

                        if ret_angle == -1:  ## Incase of error, continue loop
                            continue
                            
                        ##Calculate Distance, Time & Average Speed from (D1, D2)
                        D1_D2_distance ,D1_D2_time ,D1_D2_avg_speed  = osrm_distance_cal(dest_1,dest_2)
                        if D1_D2_distance == -1 or D1_D2_time == -1 or D1_D2_avg_speed == -1:
                            continue
                                                        
                        ##Writing all pre computed values to csv file 
                        temp_row=[source_coords,dest_1,dest_2,trip_dict[dest_1],passenger_dict[dest_1],round(ret_angle,2),source_D1_distance ,source_D1_time ,source_D1_avg_speed]
                        temp_row.extend([source_D1_distance*2,source_D1_distance*2.5,source_D1_distance*3,source_D1_distance*4,original_accepted_delay,new_50_accepted_delay])
                        temp_row.extend([D1_D2_distance ,D1_D2_time ,D1_D2_avg_speed,D1_D2_distance*2,D1_D2_distance*2.5,D1_D2_distance*3,D1_D2_distance*4])
                        
                        writer.writerow(temp_row)
                        matchings=matchings+1
                
                    del temp_trip_dict[dest_1] ## Delete this key from the time interval dictionary,since all computations are completed
                print("Matching Count ",matchings)
                print()
    except Exception as e:
        print("Exception :", e)
     


if __name__ == '__main__':
    main()
    
