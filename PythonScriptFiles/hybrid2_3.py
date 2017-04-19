import csv
import psycopg2
from datetime import datetime,timedelta
import sys, os
import time
import operator
header_row=['source_coords','dest1_coords','dest2_coords','dest3_coords','source_D1_distance(in miles)','source_D1_time(in minutes)','poolsize_time_window','peak_nonpeak_hours','pool_number','processing_time','source_D2_distance(in miles)','source_D2_time(in minutes)']
header_row.extend(['source_D3_distancw(in miles)','source_D3_time(in minutes)','total_shared_distance','total_shared_time','Matched_NoMatched'])

running_shared_total_distance = 0; running_shared_total_time=0;
without_sharing_total_distance =0 ; without_sharing_total_time=0;total_no_of_rides_in_run=0;
original_trips_data={}
two_pairs={}
three_pairs={}
cursor = None
all_paired_dest = set()
final_pairing = {};
final_single_rides={}
single_trip_distance = 0; single_trip_time =0;
csv_list=[]
processing_time={}
processing_time_running = 0
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
        global running_shared_total_distance; global running_shared_total_time;
        if cursor is None:
            cursor = create_db_conn()

        s_coords = trip[1]; d1_coords = trip[2];  pickup_time = trip[3];d1_passenger_count = trip[4] ; s_d1_dist = trip[5] ;s_d1_time = trip[6];s_d1_avg_speed=trip[7]
        d1_original_accepted_delay = trip[12] ;original_cost_2= trip[8]; original_cost_25= trip[9];original_cost_3 = trip[10] ;original_cost_4= trip[11];d1_new_50_accepted_delay=trip[12]
 
        ##Select all precomputed rows from table for destination D1, with angle >=30, ordered by maximum original delay (desc)
        a= "select * from taxisharing.janprecomputeddata where dest1_coords ='{0}' and ret_angle <= 30 order by original_accepted_delay desc;".format(d1_coords)
        cursor.execute(a)
        precomputed_rows = cursor.fetchall()
        if len(precomputed_rows) == 0 or precomputed_rows == None:
            return

        for row in precomputed_rows:
            d2_coords = row[3]
            d2_passenger_count = row[5]
            d1_d2_time = row[16]
            d1_d2_dist= row[15]

            if d1_coords in all_paired_dest:
                continue
            if d2_coords in all_paired_dest:
                continue

            if d1_coords in final_pairing or d1_coords in final_pairing.values():
                continue

            if d2_coords in final_pairing or d2_coords in final_pairing.values():
                continue
            if d1_coords not in original_trips_data  and d2_coords not in original_trips_data:
                continue
            
            ##C1: Passenger Count Constraint
            passenger_count = d1_passenger_count + d2_passenger_count
            if passenger_count > 4:
                continue

            #Retriving the orginal S-D2 distance and time from trips tables
            stmt= "select * from taxisharing.JanNewTripRequests where dest1_coords ='{0}'".format(d2_coords)
            cursor.execute(stmt)
            d2_rows = cursor.fetchall()
            
            if len(d2_rows) == 0 or d2_rows == None:
                continue
            
            d2_trip=d2_rows[0]
            d2_pickup_time = d2_trip[3]; s_d2_dist = d2_trip[5] ;s_d2_time = d2_trip[6];s_d2_avg_speed=d2_trip[7]
            d2_original_accepted_delay = d2_trip[12] ;d2_original_cost_2= d2_trip[8]; d2_original_cost_25= d2_trip[9];d2_original_cost_3 = d2_trip[10] ;d2_original_cost_4= d2_trip[11];d2_new_50_accepted_delay=trip[12]

            #Ordering: S-D2-D1
            accepted_delay = d1_original_accepted_delay     ##Based on distance
            source_travel_time = s_d1_time
            total_travel_time = s_d2_time + d1_d2_time
            total_travel_distance = s_d2_dist + d1_d2_dist

            ##C3: delay time Constraint
            if(total_travel_time > (source_travel_time + accepted_delay)):
                continue
            
            ##Calling method to try to find third match:
            val = third_pairing(d1_coords,d2_coords,passenger_count,d2_trip,d1_d2_time,d1_d2_dist,s_d1_time,s_d1_dist,s_d2_dist,d1_original_accepted_delay)
            
            if not val:
                prostime = processing_time[original_trips_data[d1_coords][4]]
                two_pairs[d2_coords] = ''
                #match d1,d2
                final_pairing[d1_coords] = d2_coords
                all_paired_dest.add(d2_coords)
                all_paired_dest.add(d1_coords)
                running_shared_total_distance = running_shared_total_distance + s_d1_dist + d1_d2_dist
                running_shared_total_time = running_shared_total_time + s_d1_time + d1_d2_time
                #Writing to CSV list
                temp_row=['(-73.785924, 40.645134)',d1_coords,d2_coords,'',s_d1_dist,s_d1_time,original_trips_data[d1_coords][2],original_trips_data[d1_coords][3],original_trips_data[d1_coords][4],prostime,s_d2_dist,s_d2_time,'','',(s_d1_dist + d1_d2_dist),(s_d1_time + d1_d2_time),'2_Matched']
                csv_list.extend([temp_row])
                
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return None
    

def third_pairing(d1_coords,d2_coords,existing_passenger_count,d2_trip,d1_d2_time,d1_d2_dist,s_d1_time,s_d1_dist,s_d2_dist,d1_original_accepted_delay):

    try:
            
        global running_shared_total_distance; global running_shared_total_time;
        ##Select all precomputed rows from table for destination D1, with angle >=30, ordered by maximum original delay (desc)
        a= "select * from taxisharing.janprecomputeddata where dest1_coords ='{0}' and ret_angle <= 30 and dest2_coords !='{1}' order by original_accepted_delay desc;".format(d2_coords,d1_coords)
        cursor.execute(a)
        precomputed_rows = cursor.fetchall()
        if len(precomputed_rows) == 0 or precomputed_rows == None:
            return False

        for row in precomputed_rows:                             
            d3_coords = row[3]
            d3_passenger_count = row[5]
            d2_d3_time = row[16]
            d2_d3_dist= row[15]

            prostime =processing_time[original_trips_data[d1_coords][4]]
            
            if d1_coords in final_pairing or d1_coords in final_pairing.values():
                continue
            if d2_coords in final_pairing or d2_coords in final_pairing.values():
                continue
            if d3_coords in final_pairing or d3_coords in final_pairing.values():
                continue
            if d1_coords in all_paired_dest:
                continue
            if d2_coords in all_paired_dest:
                continue
            if d3_coords in all_paired_dest:
                continue

            if d1_coords not in original_trips_data  and d2_coords not in original_trips_data and d3_coords not in original_trips_data:
                continue
            
            str1 = [d1_coords,d2_coords]
            str2 = [d1_coords,d3_coords]
            str3 = [d2_coords,d3_coords]
            str4 = [d2_coords,d1_coords]
            str5 = [d3_coords,d1_coords]
            str6 = [d3_coords,d2_coords]

            if str1 in final_pairing.values() or str2 in final_pairing.values()  or str3 in final_pairing.values()  or str4 in final_pairing.values()  or str5 in final_pairing.values()  or str6 in final_pairing.values() :
                continue
            if d1_coords == d2_coords or d2_coords == d3_coords  or d1_coords == d3_coords:
                continue
            
            ##C1: Passenger Count Constraint
            passenger_count = existing_passenger_count + d3_passenger_count 
            if passenger_count > 4:
                continue

            #Retriving the orginal S-D3 distance and time from trips tables
            stmt= "select * from taxisharing.JanNewTripRequests where dest1_coords ='{0}'".format(d3_coords)
            cursor.execute(stmt)
            d3_rows = cursor.fetchall()
            
            if len(d3_rows) == 0 or d3_rows == None:
                continue
            
            d3_trip=d3_rows[0]
            d3_pickup_time = d3_trip[3]; s_d3_dist = d3_trip[5] ;s_d3_time = d3_trip[6];d3_original_accepted_delay = d3_trip[12];

            #Ordering: S-D3-D2-D1
            ##t(S-D3-D2) <= t(S-D2) + accept_delay(D2)
            ## t(S-D3-D2) + t(D2-D1) <= t(S-D1) + acct_delay (D1)
            s_d2_time = d2_trip[6];
            d2_original_accepted_delay = d2_trip[12] 
            
            t1 = s_d3_time + d2_d3_time
            if t1 <= (s_d2_time + d2_original_accepted_delay):
                if t1 + d1_d2_time <= s_d1_time + d1_original_accepted_delay: #(Match Found)
                    total_travel_time = s_d3_time + d2_d3_time + d1_d2_time
                    total_travel_distance = s_d3_dist + d2_d3_dist + d1_d2_dist
                    temp_li=[d3_coords,total_travel_time,total_travel_distance]
                    #match d1,d2,d3
                    three_pairs[d3_coords]=''
                    final_pairing[d3_coords] = [d1_coords,d2_coords]
                    all_paired_dest.add(d3_coords)
                    all_paired_dest.add(d2_coords)
                    all_paired_dest.add(d1_coords)
                    running_shared_total_distance = running_shared_total_distance + total_travel_distance
                    running_shared_total_time = running_shared_total_time + total_travel_time
                    #Writing to CSV list
                    temp_row=['(-73.785924, 40.645134)',d1_coords,d2_coords,d3_coords,s_d1_dist,s_d1_time,original_trips_data[d1_coords][2],original_trips_data[d1_coords][3],original_trips_data[d1_coords][4],prostime,s_d2_dist,s_d2_time,s_d3_dist,s_d3_time,total_travel_distance,total_travel_time,'3_Matched']
                    csv_list.extend([temp_row])
           
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return False
    return True

def print_values(): 
    try:
        global single_trip_distance; global single_trip_time;
        new_final_single_rides={}

        ##Any unmatched trips, add to single ride dict
        for i in original_trips_data:
            prostime = processing_time[original_trips_data[i][4]]
            if i not in all_paired_dest:
                final_single_rides[i] = 'Could not find match'
                single_trip_distance = single_trip_distance + original_trips_data[i][0]
                single_trip_time = single_trip_time + original_trips_data[i][1]
                #Writing to CSV list
                temp_row=['(-73.785924, 40.645134)',i,'','',original_trips_data[i][0],original_trips_data[i][1],original_trips_data[i][2],original_trips_data[i][3],original_trips_data[i][4],prostime,'','','','','','','SingleRides']
                csv_list.extend([temp_row])
                
        print("************************************")
        print("Total Number of Unique Rides Considered (Without Duplicates):", len(original_trips_data))
        print()
        print("Number of total matches", len(final_pairing))
        print("Number of no matches found(Single Trips)", len(final_single_rides))
        print("Number of k=2 Pair Matches", len(two_pairs))
        print("Number of k=3 Pair Matches", len(three_pairs))
        print("Number of rides matched by algorithm", (len(three_pairs) *3 + len(two_pairs) *2 + len(final_single_rides)))
        
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
        saving = (len(three_pairs) *3 + len(two_pairs) *2 )/total_no_of_rides_in_run
        print("PERCENTAGE OF SHARED vs NON-SHARED",round(saving*100,2))
        print("************************************")
        print()
        print("************************************")
        print("Algorithm Matching Processing Time:", processing_time_running)
        print("************************************")
        print()        
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
 
def write_to_csv():
    with open('/Users/apple/Desktop/TaxiRideSharing/Taxi Cleaned Data/Final_Output_jan_3pass.csv', 'w',encoding='ISO-8859-1',newline='') as csvwriterfile:
    ##with open('C:/Users/ykutta2/Desktop/TaxiSharing/Taxi Cleaned Data/Final_Output_jan.csv','w',encoding='ISO-8859-1',newline='') as csvwriterfile:
        writer = csv.writer(csvwriterfile, dialect='excel')
        writer.writerow(header_row)
        writer.writerows(csv_list)
    
def main():
    try:
        global cursor;global processing_time_running;
        global total_no_of_rides_in_run; global without_sharing_total_distance; global without_sharing_total_time; 
        if cursor is None:
            cursor = create_db_conn()
            
        cursor.execute("""select min(pickup_time) from taxisharing.JanNewTripRequests;""")
        rows = cursor.fetchall()
        starttime=rows[0][0]
        print("Start Time:" , starttime)

        cursor.execute("""select max(pickup_time) from taxisharing.JanNewTripRequests;""")
        rows1 = cursor.fetchall()
        endtime = rows1[0][0]
        print("End Time: ", endtime)
        print("************************************")

        cur_start_time=starttime
        counter =0
        hours=''
        while(endtime > cur_start_time):
            tic=time.clock()
            counter = counter + 1
            processing_time[counter]=tic
            cur_end_time= cur_start_time  + timedelta(minutes=4,seconds=59)

            ##Select rows from Database within 5 minute intervals
            cursor.execute("select * from taxisharing.JanNewTripRequests where pickup_time between %s and %s order by JanNewTripRequests.original_accepted_delay desc",(cur_start_time, cur_end_time))
            time_window = cursor.fetchall()
            print("Current Time Window: ",cur_start_time,cur_end_time)
            print("Number of rides in time windown:", len(time_window))
            total_no_of_rides_in_run =  total_no_of_rides_in_run + len(time_window)
            if len(time_window) >=35:
                hours='peak'
            else:
                hours='nonpeak'
            for trip in time_window:
                d1_coords = trip[2]
                original_trips_data[d1_coords] = [trip[5] , trip[6],len(time_window),hours,counter]     ##FORMAT : [s_d1_dist, s_d1_time,poolsize,peaknonpeak,poolnumber]
                without_sharing_total_distance = without_sharing_total_distance + trip[5]
                without_sharing_total_time = without_sharing_total_time + trip[6]
                find_pairing(trip)
                
            cur_start_time = cur_end_time + timedelta(seconds=1)
            toc=time.clock()
            processing_time[counter]=toc-tic
            processing_time_running = processing_time_running + processing_time[counter]
            print("Total Processing Time for this Time Window", toc-tic)
            print("************************************")
            if counter == 5:
                break  #Delete to run for all rides in time window
        print(endtime)
        print(cur_end_time)

        print("************************************")
        print("Running of Algorithm Completed")
        print("Calculating Savings & Printing Results")
        print("************************************")
        print()
        print_values()
        write_to_csv()
        
    except Exception as e:
        print("Exception :", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
if __name__ == '__main__':
    main()
    
