#Set Options and install necessary packages
options("repos" = c(CRAN = "http://cran.r-project.org/"))
install.packages("dplyr")
install.packages("stringr")
install.packages("lubridate")
install.packages("rgeos") # Spatial package
install.packages("sp") # Spatial package
install.packages("maptools") # Spatial package
install.packages("ggmap")
install.packages("ggplot2")
install.packages("gridextra") #for putting plots side by side
install.packages("ggrepel") # avoid text overlap in plots
install.packages("tidyr")
install.packages("seriation") # package for reordering a distance matrix

# Set working directory
setwd("D:/Raw Data/NYC Taxi 2016") # data folder location

#load necessary packages
options(max.print = 1000, scipen = 999, width = 100)
library(RevoScaleR)
rxOptions(reportProgress = 1) # reduces the amount of output RevoScaleR produces
library(dplyr)
options(dplyr.print_max = 200)
options(dplyr.width = Inf) # shows all columns of a tbl_df object
library(stringr)
library(lubridate)
library(rgeos) # spatial package
library(sp) # spatial package
library(maptools) # spatial package
library(ggmap)
library(ggplot2)
library(gridExtra) # for putting plots side by side
library(ggrepel) # avoid text overlap in plots
library(tidyr)
library(seriation) # package for reordering a distance matrix

#Loading data
#columns and their data types
col_classes <- c('VendorID' = "factor",
                 'tpep_pickup_datetime' = "character",
                 'tpep_dropoff_datetime' = "character",
                 'passenger_count' = "integer",
                 'trip_distance' = "numeric",
                 'pickup_longitude' = "numeric",
                 'pickup_latitude' = "numeric",
                 'RateCodeID' = "factor",
                 'store_and_fwd_flag' = "factor",
                 'dropoff_longitude' = "numeric",
                 'dropoff_latitude' = "numeric",
                 'payment_type' = "factor",
                 'fare_amount' = "numeric",
                 'extra' = "numeric",
                 'mta_tax' = "numeric",
                 'tip_amount' = "numeric",
                 'tolls_amount' = "numeric",
                 'improvement_surcharge' = "numeric",
                 'total_amount' = "numeric")

file_name <- "yellow_tripsample_2016-01.csv"

#read only top 1000 rows and columns in col_classes
sample_df <- read.csv(file_name, nrows = 1000)
head(sample_df)

#Reading whole data and creating XDF(ondisk data frames that is uniquely understood by R)
input_xdf <- 'yellow_tripdata_2016.xdf'
library(lubridate)
most_recent_date <- ymd("2016-07-01")

st <- Sys.time()
for(ii in 1:6) { # get each month's data and append it to the first month's data
  file_date <- most_recent_date - months(ii)
  input_csv <- sprintf('yellow_tripsample_%s.csv', substr(file_date, 1, 7))
  append <- if (ii == 1) "none" else "rows"
  rxImport(input_csv, input_xdf, colClasses = col_classes, overwrite = TRUE, append = append)
  print(input_csv)
}
Sys.time() - st # stores the time it took to import

#CSV Vs XDF
#XDF
input_xdf <- 'yellow_tripdata_2016.xdf'
nyc_xdf <- RxXdfData(input_xdf)
system.time(
rxsum_xdf <- rxSummary( ~ fare_amount, nyc_xdf) # provide statistical summaries for fare amount
)
rxsum_xdf

#CSV
input_csv <- 'yellow_tripsample_2016-01.csv' # we can only use one month's data unless we join the CSVs
nyc_csv <- RxTextData(input_csv, colClasses = col_classes) # point to CSV file and provide column info
system.time(
  rxsum_csv <- rxSummary( ~ fare_amount, nyc_csv) # provide statistical summaries for fare amount
)
rxsum_csv

# Checking column types
rxGetInfo(nyc_xdf, getVarInfo = TRUE, numRows = 5) # show column types and the first 10 rows

# Data Transformations
rxDataStep(nyc_xdf, nyc_xdf, 
           transforms = list(trip_percent = ifelse(fare_amount > 0 & 
            tip_amount < fare_amount, round(tip_amount * 100 / fare_amount ,0), NA)),
           overwrite = TRUE)
rxSummary(~ trip_percent, nyc_xdf)

rxSummary(~trip_percent2, nyc_xdf, 
          transforms = list(trip_percent2 = ifelse(fare_amount > 0 &
           tip_amount < fare_amount, round(tip_amount * 100/ fare_amount, 0), NA)))

rxCrossTabs(~ month:year, nyc_xdf, 
            transforms = list(year = ifelse(
                    !is.na(as.integer(substr(tpep_pickup_datetime, 5, 8))),
                      as.integer(substr(tpep_pickup_datetime, 5, 8)),
                      as.integer(substr(tpep_pickup_datetime, 6, 9))),
              month = as.integer(substr(tpep_pickup_datetime, 1,1)),
              year = factor(year, levels = 2014:2016),
              month = factor(month, levels = 1:12)))

rxCrossTabs(~ month, nyc_xdf, 
            transforms = list(
              month = as.integer(substr(tpep_pickup_datetime, 1,2)),
              month = factor(month, levels = 1:12)))

rxCrossTabs(~ year, nyc_xdf, 
            transforms = list(
              year = as.integer(substr(tpep_pickup_datetime, 5,8)),
              year = factor(year, levels = 1:12)))

rxCrossTabs( ~ month:year, nyc_xdf, 
              transforms = list(
                date = mdy_hm(tpep_pickup_datetime), 
                year = factor(year(date), levels = 2014:2016), 
                month = factor(month(date), levels = 1:12)), 
              transformPackages = "lubridate")
