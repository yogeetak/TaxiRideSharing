import json
import csv
import numpy as np
import math
import urllib
from urllib.request import urlopen
from urllib.request import Request

header_row=['source_coords','dest1_coords','dest2_coords','ret_angle','source_D1_distance(in miles)','source_D1_time(in minutes)' ,'source_D1_avg_speed(per minute)']
header_row.extend(['original_cost$2','original_cost$2.5','original_cost$3','original_cost$4','original_accepted_delay'])
header_row.extend(['D1_D2_distance(in miles)','D1_D2_time(in minutes)' ,'D1_D2_avg_speed(per minute)','D1_D2_cost$2','D1_D2_cost$2.5','D1_D2_cost$3','D1_D2_cost$4'])

unique_dest=set()
global_pair_sets=set()
source_coords= (-73.77685547,40.64508438)


def create_unique_dest_list():
    ##with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/taxi1000.csv', 'r') as csvreaderfile:
    with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/taxi1000.csv', 'r') as csvreaderfile:
        reader = csv.DictReader(csvreaderfile)
        row1=next(reader)
        unique_dest=set() 
        for row in reader:
            ##source_coords=  ( "(" + row["pickup_latitude"] +"," + row["pickup_longitude"] + ")" )                                                        
            dest_coords=    (float(row["dropoff_latitude"]) , float (row["dropoff_longitude"]) )
            unique_dest.add(dest_coords)
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

        ##url = "http://router.project-osrm.org/route/v1/driving/"+str(d1_latitude) +","+str(d1_longitude)+";" +str(d2_latitude) +","+str(d2_longitude)+";"
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
    unique_dest=create_unique_dest_list()
    
    print("****************************************")
    print("Length of Unique Destinations in File: ", len(unique_dest))
    print("****************************************")
   
    ##Opening csv file to write pre computed data
    ##with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/PreComputed_taxi1000.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
    with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/PreComputed_taxi1000.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
        writer = csv.writer(csvwriterfile, dialect='excel')
        writer.writerow(header_row)

        for dest_1 in unique_dest:
            ##Calculate Distance, Time & Average Speed from (S, D1)
            source_D1_distance ,source_D1_time ,source_D1_avg_speed  = osrm_distance_cal(source_coords,dest_1)
            ##print("Source and d1 values: ", source_D1_distance ,source_D1_time ,source_D1_avg_speed )
            if source_D1_distance == -1 or source_D1_time == -1 or source_D1_avg_speed == -1:
                continue
            
            for dest_2 in unique_dest:
                ##Dont calculate for same destinations  - (S,D1,D1)
                if(dest_1 == dest_2):
                    continue
                
                ## If the pair of (S,D1,D2) is already in the set then continue without calculation
                if (source_coords,dest_1,dest_2) in global_pair_sets:
                    continue
                
                ## If the pair of (S,D2,D1) is already in the set then continue without calculation
                if (source_coords,dest_2,dest_1) in global_pair_sets:
                    continue

                ##Calculate Angles of datapoints (S,D1,D2) - The points in tuple latitude/longitude degrees space
                ret_angle = cal_angle(source_coords,dest_1,dest_2)

                if ret_angle == -1:  ## Incase of error, continue loop
                    continue
                    
                ##Calculate Distance, Time & Average Speed from (D1, D2)
                D1_D2_distance ,D1_D2_time ,D1_D2_avg_speed  = osrm_distance_cal(dest_1,dest_2)
                if D1_D2_distance == -1 or D1_D2_time == -1 or D1_D2_avg_speed == -1:
                    continue

                                                
                ##Calculating Acceptable Delay
                if source_D1_distance >= 5:
                    original_accepted_delay = (source_D1_time * 50)/100
                else:
                    original_accepted_delay = (source_D1_time * 30)/100
                
                ##Writing all pre computed values to csv file
                temp_row=[source_coords,dest_1,dest_2,round(ret_angle,2),source_D1_distance ,source_D1_time ,source_D1_avg_speed]
                temp_row.extend([source_D1_distance*2,source_D1_distance*2.5,source_D1_distance*3,source_D1_distance*4,original_accepted_delay])
                temp_row.extend([D1_D2_distance ,D1_D2_time ,D1_D2_avg_speed,D1_D2_distance*2,D1_D2_distance*2.5,D1_D2_distance*3,D1_D2_distance*4])
                
                writer.writerow(temp_row)

                global global_pair_sets
                global_pair_sets.add((source_coords,dest_1,dest_2))



if __name__ == '__main__':
    main()
    
