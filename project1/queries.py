queries = ["" for i in range(0, 11)]

### 0. List all airport codes and their cities. Order by the city name in the increasing order. 
### Output column order: airportid, city

queries[0] = """
select airportid, city 
from airports
order by city;
"""

### 1. Write a query to find the names of the customers whose names are at least 15 characters long, and the second letter in the  name is "l".
### Order by name.
queries[1] = """
select name 
from customers 
where char_length(name) >=15 and position('l' in name) = 2 order by name;
"""


### 2. Write a query to find any customers who flew on their birthday.  Hint: Use "extract" function that operates on the dates. 
### Order output by Customer Name.
### Output columns: all columns from customers
queries[2] = """
select customerid, name, birthdate, frequentflieron 
from customers natural join flewon 
where extract(day from birthdate) = extract(day from flightdate) and extract(month from birthdate) = extract(month from flightdate) order by name;
"""

### 3. Write a query to generate a list: (source_city, source_airport_code, dest_city, dest_airport_code, number_of_flights) for all source-dest pairs with at least 2 flights. 
### Order first by number_of_flights in decreasing order, then source_city in the increasing order, and then dest_city in the increasing order.
### Note: You must generate the source and destination cities along with the airport codes.
queries[3] = """
with count_table as (select source, dest, count(*) from flights group by source, dest), 
source_city as (select city as source_city, source from flights join airports on (source = airportid)), 
dest_city as (select city as dest_city, dest from flights join airports on (dest = airportid)) 
select distinct source_city, source, dest_city, dest, count 
from count_table natural join source_city natural join dest_city 
where count >= 2 order by count DESC;
"""

### 4. Find the name of the airline with the maximum number of customers registered as frequent fliers.
### Output only the name of the airline. If multiple answers, order by name.
queries[4] = """
with freq_table as (select frequentflieron, count(*) from customers group by frequentflieron), 
airline_name_table as (select name, frequentflieron, count from freq_table join airlines on (frequentflieron = airlineid)) 
select name from airline_name_table 
where count = (select max(count) from airline_name_table);
"""

### 5. For all flights from OAK to ATL, list the flight id, airline name, and the 
### duration in hours and minutes. So the output will have 4 fields: flightid, airline name,
### hours, minutes. Order by flightid.
### Don't worry about timezones -- assume all times are reported using the same timezone.
queries[5] = """
with flight_data as (select flightid, airlineid, source, dest, extract (hour from (local_arrival_time - local_departing_time)) as hour, 
extract (minute from (local_arrival_time - local_departing_time)) as minutes from flights where source = 'OAK' and dest = 'ATL') 
select flightid, name, hour, minutes 
from flight_data natural join airlines;
"""

### 6. Write a query to find all the empty flights (if any); recall that all the flights listed
### in the flights table are daily, and that flewon contains information for a period of 9
### days from August 1 to August 9, 2016. For each such flight, list the flightid and the date.
### Order by flight id in the increasing order, and then by date in the increasing order.
queries[6] = """
with flight_table as (select distinct flightid from flights),
date_table as (select * from generate_series('2016-08-01', '2016-08-09', interval '1 day') as dates), new_date_table as (select date(dates) as dates from date_table),
ref_table as (select * from flight_table cross join new_date_table order by flightid, dates ASC)
select * from ref_table except select flightid, flightdate from flights natural join flewon order by flightid, dates ASC;;
"""

### 7. Write a query to generate a list of customers who don't list Southwest as their frequent flier airline, but
### actually flew the most (by number of flights) on that airline.
### Output columns: customerid, customer_name
### Order by: customerid
queries[7] = """
with count_table as 
(select customerid, name, frequentflieron, airlineid, count(*) 
from customers natural join flewon natural join flights group by customerid, airlineid), 
max_table as (select m.customerid, m.name, m.frequentflieron, m.airlineid, t.mx from 
(select name, max(count) as mx from count_table group by name) t inner join count_table m on (m.name = t.name) and (t.mx = m.count)) 
select customerid, name 
from max_table 
where frequentflieron != 'SW' and airlineid = 'SW' order by customerid;
"""

### 8. Write a query to generate a list of customers who flew twice on two consecutive days, but did
### not fly otherwise in the 10 day period. The output should be simply a list of customer ids and
### names. Make sure the same customer does not appear multiple times in the answer. 
### Order by the customer name. 
queries[8] = """
with consec_table as 
(select customerid, name, flightdate, flightdate - lag(flightdate) over d as consec_dates 
from customers natural join flewon window d as (partition by name order by name, flightdate)), 
sum_table as (select customerid, name, sum(consec_dates) from consec_table group by customerid, name) 
select customerid, name 
from sum_table 
where sum = 1;
"""

### 9. Write a query to find the names of the customer(s) who visited the most cities in the 10 day
### duration. A customer is considered to have visited a city if he/she took a flight that either
### departed from the city or landed in the city. 
### Output columns: name
### Order by: name
queries[9] = """
with source_table as (select distinct name, source from customers natural join flewon natural join flights order by name, source), 
dest_table as (select distinct name, dest as source from customers natural join flewon natural join flights order by name, dest), 
concat_table as (select * from source_table union select * from dest_table order by name), 
count_table as (select name, count(*) from concat_table group by name order by count DESC) 
select name 
from count_table 
where count = (select max(count) from count_table);
"""


### 10. Write a query that outputs a list: (AirportID, Airport-rank), where we rank the airports 
### by the total number of flights that depart that airport. So the airport with the maximum number
### of flights departing gets rank 1, and so on. If two airports tie, then they should 
### both get the same rank, and the next rank should be skipped.
### Order the output in the increasing order by rank.
queries[10] = """
with flight_list as 
(select distinct flightid, source, dest, airlineid, local_departing_time, local_arrival_time from flights order by source), 
count_table as (select source as airlineid, count(*) from flight_list group by source order by count DESC) 
select airlineid, count, rank() over (order by count DESC) order by airlineid DESC; 
from count_table;
"""
