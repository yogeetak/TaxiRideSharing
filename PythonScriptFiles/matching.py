import csv
import psycopg2
from datetime import datetime,timedelta
import sys, os
import operator
header_row=['source_coords','dest1_coords','dest2_coords','source_D1_distance(in miles)','source_D1_time(in minutes)','source_D2_distance(in miles)','source_D2_time(in minutes)']
header_row.extend(['total_shared_distance','total_shared_time','Matched_NoMatched'])

running_shared_total_distance = 0; running_shared_total_time=0;
without_sharing_total_distance =0 ; without_sharing_total_time=0;total_no_of_rides_in_run=0;
original_trips={};original_trips_data={}
final_no_pairing ={}
temp_matching_dict={}
cursor = None
final_pairing = {}
final_single_rides={}
single_trip_distance = 0; single_trip_time =0;
csv_list=[]
def create_db_conn():
    try:
        global cursor
        connect_str = "dbname='cs581' user='postgres' host='localhost' " + \
                  "password='password'"
        conn = psycopg2.connect(connect_str)    
        cursor = conn.cursor()
        
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        
    return cursor
    
def find_pairing(trip):
    try:
        candidates=[]
        message= ' '
        global cursor
        if cursor is None:
            cursor = create_db_conn()

        s_coords = trip[1]; d1_coords = trip[2];  pickup_time = trip[3];d1_passenger_count = trip[4] ; s_d1_dist = trip[5] ;s_d1_time = trip[6];s_d1_avg_speed=trip[7]
        d1_original_accepted_delay = trip[12] ;original_cost_2= trip[8]; original_cost_25= trip[9];original_cost_3 = trip[10] ;original_cost_4= trip[11];d1_new_50_accepted_delay=trip[12]
 
        ##Select all precomputed rows from table for destination D1
        a= "select * from taxisharing.febprecomputeddata where dest1_coords ='{0}' and ret_angle <= 30 order by original_accepted_delay asc;".format(d1_coords)
        cursor.execute(a)
        precomputed_rows = cursor.fetchall()
        if len(precomputed_rows) == 0 or precomputed_rows == None:
            message = d1_coords + ":Not present in PreComputed rows or does not have angle <= 30"
            candidates = [s_d1_dist, s_d1_time]
            return candidates,message

        for row in precomputed_rows:
            d2_coords = row[3]
            d2_passenger_count = row[5]
            d1_d2_time = row[16]
            d1_d2_dist= row[15]
            
            ##C1: Passenger Count Constraint
            passenger_count = d1_passenger_count + d2_passenger_count
            if passenger_count > 4:
                message = d1_coords + ":Passenger constraint not satisfied for trip"
                candidates = None
                continue

            #Retriving the orginal S-D2 distance and time from trips tables
            stmt= "select * from taxisharing.FebNewTripRequests where dest1_coords ='{0}'".format(d2_coords)
            cursor.execute(stmt)
            d2_rows = cursor.fetchall()
            
            if len(d2_rows) == 0 or d2_rows == None:
                message = d1_coords + ": D2: " +d2_coords + "row not found in NewTripsRequests table"
                candidates = None
                continue
            
            d2_trip=d2_rows[0]
            d2_pickup_time = d2_trip[3]; s_d2_dist = d2_trip[5] ;s_d2_time = d2_trip[6];s_d2_avg_speed=d2_trip[7]
            d2_original_accepted_delay = d2_trip[12] ;d2_original_cost_2= d2_trip[8]; d2_original_cost_25= d2_trip[9];d2_original_cost_3 = d2_trip[10] ;d2_original_cost_4= d2_trip[11];d2_new_50_accepted_delay=trip[12]
        
            ##Reorder destinations D1 and D2
            if s_d2_dist > s_d1_dist:                           #Ordering: S-D1-D2
                accepted_delay = d2_original_accepted_delay   ##Based on Distance
                ##accepted_delay = d2_new_50_accepted_delay        ##50 % direct
                source_travel_time = s_d2_time
                total_travel_time = s_d1_time + d1_d2_time
                total_travel_distance = s_d1_dist + d1_d2_dist
            else:                                               #Ordering: S-D2-D1
                accepted_delay = d1_original_accepted_delay     ##Based on distance
                ##accepted_delay = d1_new_50_accepted_delay     ##50% direct
                source_travel_time = s_d1_time
                total_travel_time = s_d2_time + d1_d2_time
                total_travel_distance = s_d2_dist + d1_d2_dist

            ##C3: delay time Constraint
            if(total_travel_time > (source_travel_time + accepted_delay)):
                message = d1_coords + ":Delay constraint failed"
                candidates = None
                continue
            
            ##C4: Total savings
            saving= s_d1_dist + s_d2_dist - total_travel_distance
            ##FORMAT : D2, Saving (distance), total_travel_distance,total_travel_time,no_rideshare_travel_dist,no_rideshare_travel_time
            no_rideshare_travel_dist = s_d1_dist + s_d2_dist
            no_rideshare_travel_time = s_d1_time + s_d2_time
            val=[d2_coords, saving,total_travel_distance,total_travel_time,no_rideshare_travel_dist,no_rideshare_travel_time]
            if candidates is None:
                candidates = [val]
            else:
                candidates.extend([val])
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return None,None
    
    if candidates is not None and len(candidates) > 0:
        message="Matched"
    return candidates, message

def prepare_final_matching(t):
    global running_shared_total_distance; global running_shared_total_time; global without_sharing_total_distance;
    global without_sharing_total_time; global single_trip_time; global single_trip_distance;
    
    try:
        if (t in final_pairing or t in final_pairing.values()):  ##If already paired, return
            return
            
        if original_trips[t] == 'Matched':         
            total_travel_distance = 0
            total_travel_time = 0
            no_rideshare_travel_dist = 0
            no_rideshare_travel_time = 0
            d2=''

            if len(temp_matching_dict[t]) == 1:  ##Implies only one candidate pairing
                d2= temp_matching_dict[t][0][0]
                total_travel_distance = temp_matching_dict[t][0][2]
                total_travel_time = temp_matching_dict[t][0][3]
                no_rideshare_travel_dist = temp_matching_dict[t][0][4]
                no_rideshare_travel_time = temp_matching_dict[t][0][5]

            else: # implies more than one candidate pairing, select the pairing with maximum saving
                curr_saving=0
                for cands in temp_matching_dict[t]:
                    if cands[0] in final_pairing or cands[0] in final_pairing.values():
                        continue
            
                    temp_curr_saving = cands[1]
                    if(temp_curr_saving > curr_saving):
                        d2= cands[0]
                        curr_saving = temp_curr_saving
                        total_travel_distance = cands[2]
                        total_travel_time = cands[3]
                        no_rideshare_travel_dist = cands[4]
                        no_rideshare_travel_time = cands[5]

            if d2 =='':
                poss= [x[0] for x in temp_matching_dict[t]]
                final_single_rides[t] = "Probable Match already taken, no other matches found from candidates :" + str(poss)
                single_trip_distance    =   single_trip_distance    +   original_trips_data[t][0]
                single_trip_time        =   single_trip_time        +   original_trips_data[t][1]
                ##Writing to CSV list
                temp_row=['(-73.785924, 40.645134)',t ,' ',original_trips_data[t][0],original_trips_data[t][1],' ',' ','','',final_single_rides[t]]
                csv_list.extend([temp_row])
                return 

            if d2 in final_pairing.values() or d2 in final_pairing:
                poss= [x[0] for x in temp_matching_dict[t]]
                final_single_rides[t] = "Probable Match already taken, no other matches found from candidates :" + str(poss)
                single_trip_distance    =   single_trip_distance    +   original_trips_data[t][0]
                single_trip_time        =   single_trip_time        +   original_trips_data[t][1]
                ##Writing to CSV list
                temp_row=['(-73.785924, 40.645134)',t ,' ',original_trips_data[t][0],original_trips_data[t][1],' ',' ','','',final_single_rides[t]]
                csv_list.extend([temp_row])
                return 
            
            #If d2 is not present in this time window then neglect the matching
            if d2 not in original_trips_data:
                return
            
            ##Adding selected D2 as FINAL PAIRING, Removing from single ride dictionary
            final_pairing[t] = d2
            if t in final_single_rides:
                del final_single_rides[t]
            if d2 in final_single_rides:
                del final_single_rides[d2]
                
            running_shared_total_distance   =   running_shared_total_distance   +   total_travel_distance
            running_shared_total_time       =   running_shared_total_time       +   total_travel_time
            without_sharing_total_distance  =   without_sharing_total_distance  +   no_rideshare_travel_dist
            without_sharing_total_time      =   without_sharing_total_time      +   no_rideshare_travel_time

            ##Writing to CSV list
            temp_row=['(-73.785924, 40.645134)',t,d2,original_trips_data[t][0],original_trips_data[t][1],original_trips_data[d2][0],original_trips_data[d2][1],total_travel_distance,total_travel_time,original_trips[t]]
            csv_list.extend([temp_row])

        else: ##Non Matched - Single Rides
            final_single_rides[t]   =   original_trips[t]
            single_trip_distance    =   single_trip_distance    +   original_trips_data[t][0]
            single_trip_time        =   single_trip_time        +   original_trips_data[t][1]

            ##Writing to CSV list
            #'source_coords','dest1_coords','dest2_coords','source_D1_distance(in miles)','source_D1_time(in minutes)','total_shared_distance','total_shared_time','Matched\NoMatched'
            temp_row=['(-73.785924, 40.645134)',t ,' ',original_trips_data[t][0],original_trips_data[t][1],' ',' ','','',original_trips[t]]
            csv_list.extend([temp_row])
        
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return 
    return 

def print_values(): 
    try:
        new_final_single_rides={}
        ##Any unaccounted trips, add to single ride dict
        for i in original_trips:
            if i not in final_pairing and i not in final_pairing.values() and i not in final_single_rides:
                final_single_rides[i] = 'Could not find match'
                
        ##Removing duplicates that are matched but also in single rides dict
        output_final_single_rides = set(final_single_rides)
        output_final_single_rides.update(final_pairing.values())
        output_final_single_rides.update(final_pairing.keys())
        if len(output_final_single_rides) !=  (len(final_pairing) + len(final_pairing.values()) + len(final_single_rides)):
            print("************************************")
            print("DUPLICATE VALUES IN SINGLE RIDES AND SHARED RIDES")
            print("************************************")

            temp_final_single_rides = final_single_rides.copy()
            for val in temp_final_single_rides:
                if val in temp_final_single_rides:
                    del final_single_rides[val]
                if temp_final_single_rides[val] in temp_final_single_rides.values():
                    del final_single_rides[val]
            
        a=set(final_pairing.values())
        if len(final_pairing) != len(a):
            print("************************************")
            print("DUPLICATE VALUES POSSIBLE IN FINAL PAIRING SET")
            print("************************************")

        b=set(final_single_rides)
        if len(final_single_rides) != len(b):
            print("************************************")
            print("DUPLICATE VALUES POSSIBLE IN FINAL SINGLE RIDES SET")
            print("************************************")
        
        print("************************************")
        print("Total Number of Rides Considered (With Duplicates):", total_no_of_rides_in_run)
        print("Total Number of Unique Rides Considered (Without Duplicates):", len(original_trips))
        print("Matched & Single Recieved from algorithm:", (len(final_pairing)*2 + len(final_single_rides)))
        print()
        print("Number of matches", len(final_pairing))
        print("Number of no matches found(Single Trips)", len(final_single_rides))
        print()
        print("Total Distance Without Ride Sharing:",without_sharing_total_distance)
        print("Total Time Without Ride Sharing:",without_sharing_total_time)
        print()
        print("Total Distance With Ride Sharing:",running_shared_total_distance)
        print("Total Time WithRide Sharing:",running_shared_total_time)
        print()
        print("Total Distance With Ride Sharing (Single Trips Considered):",single_trip_distance)
        print("Total Time WithRide Sharing (Single Trips Considered):",single_trip_time)
        print()
        print("************************************")
        print("TOTAL SAVING (DISTANCE in MILES) : ",(without_sharing_total_distance - running_shared_total_distance))
        print("TOTAL SAVING (TIME in MINUTES) : ",(without_sharing_total_time - running_shared_total_time))
        print("************************************")
        print()
        print("************************************")
        print("PERCENTAGE OF SHARED vs NON-SHARED",(round((((len(final_pairing)*2)/total_no_of_rides_in_run) *100),2)))
        print("************************************")
        print()
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
 
def write_to_csv():
    ##with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/Final_Output_jan.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
    with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/Final_Output_feb.csv','w',encoding='ISO-8859-1',newline='') as csvwriterfile:
        writer = csv.writer(csvwriterfile, dialect='excel')
        writer.writerow(header_row)
        writer.writerows(csv_list)
    
def main():
    try:
        global cursor
        global total_no_of_rides_in_run
        if cursor is None:
            cursor = create_db_conn()
            
        cursor.execute("""select min(pickup_time) from taxisharing.FebNewTripRequests;""")
        rows = cursor.fetchall()
        starttime=rows[0][0]
        print("Start Time:" , starttime)

        cursor.execute("""select max(pickup_time) from taxisharing.FebNewTripRequests;""")
        rows1 = cursor.fetchall()
        endtime = rows1[0][0]
        print("End Time: ", endtime)
        print("************************************")

        cur_start_time=starttime
        counter =0
        while(endtime > cur_start_time):
            counter = counter + 1
            cur_end_time= cur_start_time  + timedelta(minutes=4,seconds=59)

            ##Select rows from Database within 5 minute intervals
            cursor.execute("select * from taxisharing.FebNewTripRequests where pickup_time between %s and %s order by FebNewTripRequests.pickup_time asc",(cur_start_time, cur_end_time))
            time_window = cursor.fetchall()
            print("Current Time Window: ",cur_start_time,cur_end_time)
            print("Number of rides in time windown:", len(time_window))
            print("************************************")
            total_no_of_rides_in_run =  total_no_of_rides_in_run + len(time_window)
            for trip in time_window:
                d1_coords = trip[2]
                original_trips_data[d1_coords] = [trip[5] , trip[6]]     ##FORMAT : [s_d1_dist, s_d1_time]
                candidates, message = find_pairing(trip)
                original_trips[d1_coords]  = message
                
                if message is 'Matched':
                    temp_matching_dict[d1_coords] = candidates
                    
            cur_start_time = cur_end_time + timedelta(seconds=1)
            #if counter == 1:
            #break  #Delete to run for all rides in time window
        print(endtime)
        print(cur_end_time)

        print("************************************")
        print("Running of Algorithm Completed")
        print("Calculating Savings & Printing Results")
        print("************************************")
        print()

        sorted_original_trips = sorted(original_trips.items(), key=operator.itemgetter(0))
        
        for tup in sorted_original_trips:
            t=tup[0]
            res = prepare_final_matching(t)
                   
        print_values()
        write_to_csv()
    
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


if __name__ == '__main__':
    main()
    
