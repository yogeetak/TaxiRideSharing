import csv
import psycopg2
from datetime import datetime,timedelta
import sys, os

    
def main():
    try:
        running_shared_total_distance = 0; running_shared_total_time=0;
        without_sharing_total_distance =0 ; without_sharing_total_time=0;
        final_pairing={}
        final_no_pairing ={}
        matching_temp_dict={}
        no_matching_temp_dict={}
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
            cursor.execute("select * from taxisharing.newtripsrequests where pickup_time between %s and %s order by newtripsrequests.pickup_time asc",(cur_start_time, cur_end_time))
            time_window = cursor.fetchall()
            print("Number of rides in time windown:", len(time_window))
            print("************************************")
            for trip in time_window:
                matched = False
                s_coords = trip[1]; d1_coords = trip[2];  pickup_time = trip[3];d1_passenger_count = trip[4] ; s_d1_dist = trip[5] ;s_d1_time = trip[6];s_d1_avg_speed=trip[7]
                d1_original_accepted_delay = trip[12] ;original_cost_2= trip[8]; original_cost_25= trip[9];original_cost_3 = trip[10] ;original_cost_4= trip[11]

                ##Select all precomputed rows from table for destination D1
                a= "select * from taxisharing.newprecomputedtable where dest1_coords ='{0}' and ret_angle <= 30 order by original_accepted_delay asc;".format(d1_coords)
                cursor.execute(a)
                precomputed_rows = cursor.fetchall()
                if len(precomputed_rows) == 0 or precomputed_rows == None:
                    print("Not present in preComputed rows or angle > 30", d1_coords)
                    no_matching_temp_dict[d1_coords] =''
                    continue

##                print("D1:", d1_coords)
##                print("d1_passenger_count",d1_passenger_count)
##                print("s_d1_dist",s_d1_dist)
##                print("s_d1_time",s_d1_time)
##                print("d1_original_accepted_delay",d1_original_accepted_delay)
##                print("Possible Candidate Matches from PreComputed tables",len(precomputed_rows))
##                print()
                counter=0
                
                ##TODO: For desination D1 which has no d2's with angles <-30 eg: (-73.79402924, 40.65670776), try to find best match using other factors

                for row in precomputed_rows:
                    counter=counter+1
                    d2_coords = row[3]
                    d2_passenger_count = row[5]
                    d1_d2_time = row[16]
                    d1_d2_dist= row[15]
                    
                    ##C1: Passenger Count Constraint
                    passenger_count = d1_passenger_count + d2_passenger_count
                    if passenger_count > 4:
                        print("Passenger constraint not satisfied for trip:", d1_coords,d1_passenger_count,d2_coords,d2_passenger_count)
                        continue
                        
                    ##C2: Number of trips <= 2

                    #Retriving the orginal S-D2 distance and time from trips tables
                    stmt= "select * from taxisharing.newtripsrequests where dest1_coords ='{0}'".format(d2_coords)
                    cursor.execute(stmt)
                    d2_rows = cursor.fetchall()
                    
                    if len(d2_rows) == 0 or d2_rows == None:
                        continue
                    d2_trip=d2_rows[0]
                    d2_pickup_time = d2_trip[3]; s_d2_dist = d2_trip[5] ;s_d2_time = d2_trip[6];s_d2_avg_speed=d2_trip[7]
                    d2_original_accepted_delay = d2_trip[12] ;d2_original_cost_2= d2_trip[8]; d2_original_cost_25= d2_trip[9];d2_original_cost_3 = d2_trip[10] ;d2_original_cost_4= d2_trip[11]

##                    print("D2:",d2_coords)
##                    print("d2_passenger_count",d2_passenger_count)
##                    print("s_d2_dist",s_d2_dist)
##                    print("s_d2_time",s_d2_time)
##                    print("d2_original_accepted_delay",d2_original_accepted_delay)
##                    print()
                
                    ##Reorder destinations D1 and D2
                    if s_d2_dist > s_d1_dist:                               #Ordering: S-D1-D2
                        accepted_delay = d2_original_accepted_delay
                        source_travel_time = s_d2_time
                        total_travel_time = s_d1_time + d1_d2_time
                        total_travel_distance = s_d1_dist + d1_d2_dist
                    else:                                                   #Ordering: S-D2-D1
                        accepted_delay = d1_original_accepted_delay
                        source_travel_time = s_d1_time
                        total_travel_time = s_d2_time + d1_d2_time
                        total_travel_distance = s_d2_dist + d1_d2_dist

                    ##C3: delay time Constraint
                    if(total_travel_time > (source_travel_time + accepted_delay)):
                        #print("Delay constraint failed", d1_coords,d2_coords)
                        continue
                    
                    ##C4: Total savings
                    saving= s_d1_dist + s_d2_dist - total_travel_distance
                    matched = True
                    ##FORMAT : D2, Saving (distance), total_travel_distance,total_travel_time,no_rideshare_travel_dist,no_rideshare_travel_time
                    no_rideshare_travel_dist = s_d1_dist + s_d2_dist
                    no_rideshare_travel_time = s_d1_time + s_d2_time
                    val=[d2_coords, saving,total_travel_distance,total_travel_time,no_rideshare_travel_dist,no_rideshare_travel_time]
                    
                    if d1_coords in matching_temp_dict: 
                        i=matching_temp_dict[d1_coords]
                        i.extend([val])
                        matching_temp_dict[d1_coords]=i
                        
                    else:
                        matching_temp_dict[d1_coords] = [val]
           
                if not matched:
                    ##FORMAT : D1,s_d1_dist,s_d1_time
                    no_matching_temp_dict[d1_coords] =[s_d1_dist,s_d1_time]
                      
            cur_start_time = cur_end_time
            break  #Delete to run for all rides in time window


        print("************************************")
        print("Running of Algorithm Completed")
        print("Calculating Savings & Printing Results")
        print("************************************")
        print()
 
        for i in matching_temp_dict:
            if i in final_pairing : #Match of d1 already made with some other prior trip
                continue
            
            ##Adding to a final matching dictionary - ##FORMAT : D2, Saving (distance), total_travel_distance,total_travel_time

            if len(matching_temp_dict[i]) == 1:  ##Implies only one candidate pairing
                d2= matching_temp_dict[i][0][0]
                if d2 in final_pairing  or d2 in final_pairing.values():
                    continue
                final_pairing[i]= d2
                running_shared_total_distance=running_shared_total_distance+ matching_temp_dict[i][0][2]
                running_shared_total_time=running_shared_total_time+matching_temp_dict[i][0][3]
                without_sharing_total_distance=without_sharing_total_distance + matching_temp_dict[i][0][4]
                without_sharing_total_time=without_sharing_total_time + matching_temp_dict[i][0][5]
                

            else: # implies more than one candidate pairing, select the pairing with maximum saving
                curr_saving=0

                for cands in matching_temp_dict[i]:
                    d2_1= cands[0]
                    if d2_1 in final_pairing or d2_1 in final_pairing.values() :
                        continue

                    temp_curr_saving = cands[1]
                    if(temp_curr_saving > curr_saving):
                        curr_saving = temp_curr_saving
                        final_pairing[i]= d2_1
                        running_shared_total_distance=running_shared_total_distance+ matching_temp_dict[i][0][2]
                        running_shared_total_time=running_shared_total_time+matching_temp_dict[i][0][3]
                        without_sharing_total_distance=without_sharing_total_distance + matching_temp_dict[i][0][4]
                        without_sharing_total_time=without_sharing_total_time + matching_temp_dict[i][0][5]
                

        ##Remove from non-match dictionary
        for pair in final_pairing:
            print("Matching :", pair, final_pairing[pair])
            if pair in no_matching_temp_dict:
                del no_matching_temp_dict[pair]
            if final_pairing[pair] in no_matching_temp_dict:
                del no_matching_temp_dict[final_pairing[pair]]
    
        print()
        for n_pairs in no_matching_temp_dict:
            print("Non Matchings:", n_pairs)
            ##FORMAT : D1, s_d1_dist,s_d1_time
            running_shared_total_distance=running_shared_total_distance+ no_matching_temp_dict[n_pairs][0]
            running_shared_total_time=running_shared_total_time+ no_matching_temp_dict[n_pairs][1]
            without_sharing_total_distance=without_sharing_total_distance + no_matching_temp_dict[n_pairs][0]
            without_sharing_total_time=without_sharing_total_time + no_matching_temp_dict[n_pairs][1]
            

        print("************************************")
        print("Number of matches", len(final_pairing))
        print("Number of no matches found", len(no_matching_temp_dict))
        print("Total Distance Without Ride Sharing:",without_sharing_total_distance)
        print("Total Time Without Ride Sharing:",without_sharing_total_time)
        print("Total Distance With Ride Sharing:",running_shared_total_distance)
        print("Total Time WithRide Sharing:",running_shared_total_time)
        print("************************************")
        print("TOTAL SAVING (DISTANCE in MILES) : ",(without_sharing_total_distance - running_shared_total_distance))
        print("TOTAL SAVING (TIME in MINUTES) : ",(without_sharing_total_time - running_shared_total_time))

        print("************************************")
        print()
            
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


if __name__ == '__main__':
    main()
    
