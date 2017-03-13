import json
import csv
import sys
import numpy as np
import math

header_row=['']


##TODO: convert lat,long to 3 points
## Calculating the angle between three points

##a = np.array([32.49, -39.96,-3.86])
##b = np.array([31.39, -39.28, -4.66])
##c = np.array([31.14, -38.09,-4.49])
##
##ba = a - b
##bc = c - b
##
##cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
##angle = np.arccos(cosine_angle)
##
##print (np.degrees(angle))


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

pointA=(-73.776702880859375, 40.645370483398437)
pointB=(-73.776679992675781, 40.645378112792969)
##pointB=(-73.801872253417969, 40.665641784667969 )

calculate_initial_compass_bearing (pointA,pointB)

