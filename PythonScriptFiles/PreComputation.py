import json
import csv
import sys
import numpy as np
import math
import traceback


header_row=['source_coords','dest1_coords','dest2_coords','ret_angle']
unique_dest=set()
source_coords=()
global_pair_sets=set()

def create_unique_dest_list():
 
    with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/taxi1000.csv', 'r') as csvreaderfile:
    ##with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/taxi1000.csv', 'r') as csvreaderfile:
        reader = csv.DictReader(csvreaderfile)
        row1=next(reader)
        unique_dest=set() 
        """Considering all Source Latitude and Longitude points will be same"""
        global source_coords
        source_coords=( float(row1["pickup_latitude"]) , float(row1["pickup_longitude"]) )
        for row in reader:
            ##source_coords=  ( "(" + row["pickup_latitude"] +"," + row["pickup_longitude"] + ")" )                                                        
            dest_coords=    (float(row["dropoff_latitude"]) , float (row["dropoff_longitude"]) )
            unique_dest.add(dest_coords)
        return unique_dest
            
    
def main():
    
    unique_dest=create_unique_dest_list()
    print("****************************************")
    print("Length of Unique Destinations in File: ", len(unique_dest))
    print("****************************************")
   
    ##Opening csv file to write pre computed data
    with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/PreComputed_taxi1000.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
    ##with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/PreComputed_taxi1000.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
        writer = csv.writer(csvwriterfile, dialect='excel')
        writer.writerow(header_row)

        for dest_1 in unique_dest:
            for dest_2 in unique_dest:

                ##Dont calculate for same destinations  - (S,D1,D1)
                if(dest_1 == dest_2):
                    continue
                
                ## If the pair of (S,D1,D2) or (S,D2,D1) is already in the set then continue without calculation
                if (source_coords,dest_1,dest_2) in global_pair_sets:
                    print(source_coords,dest_1,dest_2)
                    continue
                
                if (source_coords,dest_2,dest_1) in global_pair_sets:
                    print(source_coords,dest_2,dest_1)
                    continue
            

                try:
                        
                    ##Calculate Angles of datapoints (S,D1,D2) - The points in tuple latitude/longitude degrees space
                    ret_angle = cal_angle(source_coords,dest_1,dest_2)

                    if ret_angle == -1:  ## Incase of error, continue loop
                        print("Exception in calculating angel for following points: ", dest_1, dest_2)
                        continue

                    temp_row=[source_coords,dest_1,dest_2,ret_angle]
                    writer.writerow(temp_row)

                    global global_pair_sets
                    global_pair_sets.add((source_coords,dest_1,dest_2))

                except:
                    print("Exception in points: ",dest_1, dest_2)
                    continue
            
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

def calculate_initial_compass_bearing(pointA, pointB):
    """
    Calculates the bearing between two points.
    The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
                  cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
    :Parameters:
      - `pointA: The tuple representing the latitude/longitude for the
        first point. Latitude and longitude must be in decimal degrees
      - `pointB: The tuple representing the latitude/longitude for the
        second point. Latitude and longitude must be in decimal degrees
    :Returns:
      The bearing in degrees
    :Returns Type:
      float
    """
    if (type(pointA) != tuple) or (type(pointB) != tuple):
        raise TypeError("Only tuples are supported as arguments")

    lat1 = math.radians(pointA[0])
    lat2 = math.radians(pointB[0])

    diffLong = math.radians(pointB[1] - pointA[1])

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
            * math.cos(lat2) * math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360
    print(compass_bearing)
    return compass_bearing


if __name__ == '__main__':
    main()
    
    ##calculate_initial_compass_bearing (pointA,pointB)
    A=(-73.776702880859375, 40.645370483398437)
    B=(-73.776679992675781, 40.645378112792969)
    C=(-73.801872253417969, 40.665641784667969)

