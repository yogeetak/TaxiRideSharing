import csv
import psycopg2
from datetime import datetime,timedelta
import sys, os

    
def main():
    try:
        connect_str = "dbname='cs581' user='postgres' host='localhost' " + \
                  "password='password'"
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()
        cursor.execute("""select min(pickup_time) from taxisharing.newtripsrequests;""")
        rows = cursor.fetchall()
        starttime=rows[0][0]
        print("Start Time:" , starttime)

        cursor.execute("""select max(pickup_time) from taxisharing.newtripsrequests;""")
        rows1 = cursor.fetchall()
        endtime = rows1[0][0]
        print("End Time: ", endtime)
        print("************************************")

        cur_start_time=starttime
        while(cur_start_time < endtime):
            cur_end_time= cur_start_time  + timedelta(minutes=5)

            ##Select rows from Database within 5 minute intervals
            cursor.execute("select * from taxisharing.newtripsrequests where pickup_time between %s and %s",
            (cur_start_time, cur_end_time))
            time_window = cursor.fetchall()
            for trip in time_window:
                
                s_coords = trip[1]; d1_coords = trip[2];  pickup_time = trip[3];d1_passenger_count = trip[4] ; s_d1_dist = trip[5] ;s_d1_time = trip[6];s_d1_avg_speed=trip[7]
                original_accepted_delay = trip[12] ;original_cost_2= trip[8]; original_cost_25= trip[9];original_cost_3 = trip[10] ;original_cost_4= trip[11]
                
                ##Select all precomputed rows from table for destination D1
                a= "select * from taxisharing.newprecomputedtable where dest1_coords =  '{0}' and ret_angle <= 30 order by pickup_time asc;".format(d1_coords)
                cursor.execute(a)
                precomputed_rows = cursor.fetchall()
                if len(precomputed_rows) == 0 or precomputed_rows == None:
                    print("Not present in preComputed rows or angle > 30", d1_coords)
                    continue
                
                ##TODO: For desination D1 which has no d2's with angles <-30 eg: (-73.79402924, 40.65670776), try to find best match using other factors

                for row in precomputed_rows:
                    d2_coords = row[2]
                    d2_passenger_count = row[5]
        
                    ##C1: Passenger Count Constraint
                    passenger_count = d1_passenger_count + d2_passenger_count
                    if passenger_count > 4:
                        print("Passenger constraint not satisfied for trip:", d1_coords,d2_coords)
                        continue
                        
                    ##C2: Number of trips <= 2

                    #Retriving the orginal S-D2 distance and time from trips tables
                    stmt= "select * from taxisharing.newtripsrequests where dest1_coords ='{0}'".format(d2_coords)
                    cursor.execute(a)
                    d2_rows = cursor.fetchall()
                    print(len(d2_rows))

                    ##Reorder destinations
                

##                    if s_d2_dist > s_d1_dist: 
##                        accepted_delay = float(row["original_accepted_delay"])  ##Should be delay of D1
##                        original_travel_time = float(row["source_D1_time(in minutes)"]) ##Should be time of S-D1
##                        total_travel_time = original_travel_time + float(row["D1_D2_time(in minutes)"])  
##
##                    else:
##                        accepted_delay = float(row["original_accepted_delay"])
##                        original_travel_time = float(row["source_D1_time(in minutes)"]) ##should be time of S-D2
##                        total_travel_time = original_travel_time + float(row["D1_D2_time(in minutes)"])
                
                
                
##                print("Source coords: ", trip[1])
##                print("dest1_coords:  ", trip[2])
##                print("pickup_time:  ", trip[3])
##                print("passenger_count:", trip[4])
##                print(" source_d1_distance_in_miles:", trip[5])
##                print(" source_d1_time_in_minutes:", trip[6])
##                print(" source_d1_avg_speed_per_minute:", trip[7])
##                print(" original_accepted_delay:", trip[12])
##                print(" original_cost_$_2:", trip[8])
##                print(" original_cost$_2_point_5:", trip[9])
##                print(" original_cost_$_3:", trip[10])
##                print(" original_cost_$_4:", trip[11])
            
            cur_start_time = cur_end_time
            break
            
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


##        ##TODO: Pick angles lesser that 30
##        ##Sort them in ascending order
##        for row in reader:
##            r_pass_count=1 ##d1 passenger count
##            d2 = row["dest2_coords"]
##
##            ##C1: Passenger Count Constraint
##            r_pass_count = r_pass_count + float(row["passenger_count"])
##            if r_pass_count > 4:
##                print("Passenger constraint not satisfied")
##                continue
##
##            ##C2: Number of trips <= 2
##
##            ##Reorder destinations
##            s_d1_dist = float(row["source_D1_distance(in miles)"])
##            s_d2_dist = 0.0
##
##            if s_d2_dist > s_d1_dist: 
##                accepted_delay = float(row["original_accepted_delay"])  ##Should be delay of D1
##                original_travel_time = float(row["source_D1_time(in minutes)"]) ##Should be time of S-D1
##                total_travel_time = original_travel_time + float(row["D1_D2_time(in minutes)"])  
##
##            else:
##                accepted_delay = float(row["original_accepted_delay"])
##                original_travel_time = float(row["source_D1_time(in minutes)"]) ##should be time of S-D2
##                total_travel_time = original_travel_time + float(row["D1_D2_time(in minutes)"])
## 
##            ##C3: delay time constraint
##            print("accepted_delay:",accepted_delay)
##            print("original_travel_time",original_travel_time)
##            print("time:", (original_travel_time + accepted_delay))
##            print("total_travel_time",total_travel_time)
##            print()
##            
##            if( total_travel_time > (original_travel_time + accepted_delay)):
##                print("delay constraint failed", d1,d2)
##                continue
##            
##            ##C4: Total savings
##            combined_distance = float(row["D1_D2_distance(in miles)"])
##            saving= s_d1_dist + s_d2_dist - combined_distance
##
##            pairing = (d1 , d2, saving)
##            print("%%%%%%%%%", saving)



if __name__ == '__main__':
    main()
    
