import json
import csv
import sys
import numpy as np

header_row=['']


##TODO: convert lat,long to 3 points
## Calculating the angle between three points

a = np.array([32.49, -39.96,-3.86])
b = np.array([31.39, -39.28, -4.66])
c = np.array([31.14, -38.09,-4.49])

ba = a - b
bc = c - b

cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))
angle = np.arccos(cosine_angle)

print (np.degrees(angle))
