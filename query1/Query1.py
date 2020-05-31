from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, from_unixtime
from pyspark.sql.types import StringType, LongType, IntegerType
import math, time


# Set app name
appName = "debsChallenge"

#create a new Spark application and get the Spark session object
spark = SparkSession.builder.appName(appName).getOrCreate()

cell_500_to_meters_lat = 0.004491556
cell_500_to_meters_lon = 0.005986   

start_lat, start_lon = 41.474937, -74.913585


def read_data(input_file):
    data = spark.read \
                  .option("inferSchema", True) \
                  .option("header", True) \
                  .csv(input_file)
    return data


def get_max_lat_lon(start_lat, start_lon, max_east_meter, max_south_meter, cell_size):
    nr_east_cells = max_east_meter / cell_size
    nr_south_cells = max_south_meter / cell_size
    
    dist_east = nr_east_cells * cell_500_to_meters_lon
    dist_south = nr_south_cells * cell_500_to_meters_lat
        
    max_lon = start_lon + dist_east
    max_lat = start_lat - dist_south
    return max_lat, max_lon



def filter_out_of_border(start_lat, start_lon, max_lat, max_lon, data):
    # Filter out entries where either pick up or drop off is out of bounds
    return data \
        .filter("pickup_latitude > " + str(max_lat)) \
        .filter("pickup_latitude < " + str(start_lat)) \
        .filter("pickup_longitude < " + str(max_lon)) \
        .filter("pickup_longitude > " + str(start_lon)) \
        .filter("dropoff_latitude > " + str(max_lat)) \
        .filter("dropoff_latitude < " + str(start_lat)) \
        .filter("dropoff_longitude < " + str(max_lon)) \
        .filter("dropoff_longitude > " + str(start_lon))



def calculate_cell(lat, lon, cell_size):
    dist_south = start_lat - lat
    dist_east = lon - start_lon

    south_cell = math.ceil(dist_south / cell_500_to_meters_lat)
    east_cell = math.ceil(dist_east / cell_500_to_meters_lon)
    
    # east.south
    return str(east_cell) + "." + str(south_cell)

get_cell = udf(calculate_cell, StringType())


def make_col_maxtime(data):
    # get the latest date as a string
    max_time = data.select("dropoff_datetime").limit(1).collect()[0]
    max_time = max_time.__getitem__("dropoff_datetime")
    print("max date: ", max_time)

    # make a column containing the latest date
    data = data.withColumn("maxtime", lit(max_time))
    return data


# calculate difference in minutes manually from datetime format "MM/dd/yyy HH:mm"
def get_time_diff(maxtime, compare_time):
    max_date, max_time = maxtime.split()
    max_month, max_day, max_year = max_date.split("/")
    max_hour, max_minute = max_time.split(":")
    
    comp_date, comp_time = compare_time.split()
    comp_month, comp_day, comp_year = comp_date.split("/")
    comp_hour, comp_minute = comp_time.split(":")
    
    diff_year = int(max_year) - int(comp_year)
    diff_month = int(max_month) - int(comp_month)
    diff_day = int(max_day) - int(comp_day)
    diff_hour = int(max_hour) - int(comp_hour)
    diff_minute = int(max_minute) - int(comp_minute)
    
    diff_minutes = diff_year * 365 * 24 * 60 + \
                diff_month * 30 * 24 * 60 + \
                diff_day * 24 * 60 + \
                diff_hour * 60 + \
                diff_minute
    
    return diff_minutes


# set the udf of calculating the difference in datetimes in minutes
minute_diff = udf(get_time_diff, IntegerType())


def textual_solution(data):
    starting_cells = data.select("starting_cell").collect()
    start = [float(val[0]) for val in starting_cells]
    
    ending_cells = data.select("ending_cell").collect()
    end = [float(val[0]) for val in ending_cells]
    
    pickup_time = data.select("pickup_datetime").collect()[0]['pickup_datetime']
    dropoff_time = data.select("dropoff_datetime").collect()[0]['dropoff_datetime']
    
    solution = pickup_time + " " + dropoff_time + " "
    
    for i, j in zip(start, end):
        solution += str(i) + " " + str(j) + " "
    
    return solution



def main():
    # read the content of the file
    input_file = "input/sorted_data/small100.csv"
    time_start = time.process_time()
    data = read_data(input_file)
    
    ##############################
    # FILTER OUT OF BOUNDS RECORDS
    ##############################
    
    # calculate maximum latitude and longitude
    # start_lat, start_lon = 41.474937, -74.913585
    max_lat, max_lon = get_max_lat_lon(start_lat, start_lon, 150000, 150000, 500)
    
    # filter out records that have pickups or dropoffs outside of considered area
    data = filter_out_of_border(start_lat, start_lon, max_lat, max_lon, data)
    
    ########################
    # FILTER OUT OLD RECORDS
    ########################
    
    # order by dropoff time
    data = data.orderBy(data.dropoff_datetime.desc())
    
    # make a column with the latest date
    data = make_col_maxtime(data)
    
    # create a column with the difference in minutes between the dropoff_time and maximum time. Save only records where 
    # difference in time is less or equal to 30 minutes
    data = data.withColumn("minute_diff", minute_diff(data.maxtime, data.dropoff_datetime))
    
    # filter out records that have dropoff time older than 30 minutes
    data = data.filter("minute_diff <= 30")
    
    #################
    # CALCULATE CELLS
    #################
    
    # calculate starting cell
    data = data.withColumn("starting_cell", get_cell(data["pickup_latitude"], data["pickup_longitude"], lit(500)))
    
    # calculate ending cell
    data = data.withColumn("ending_cell", get_cell(data["dropoff_latitude"], data["dropoff_longitude"], lit(500)))
    
    #################
    # CALCULATE ROUTE
    #################
    
    solution = data.groupBy("starting_cell", "ending_cell", "pickup_datetime", "dropoff_datetime").count()
    solution = solution.withColumnRenamed("count", "route")
    solution = solution.orderBy(
        solution.dropoff_datetime,
        solution.route, 
        ascending=False)
    solution = solution.limit(10)
    solution.show()

    sol = textual_solution(solution)
    delay = time.process_time() - time_start
    print(sol + str(delay))
    
main()