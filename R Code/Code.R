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
sample_df <- read.csv(file_name, nrows = 1000, colClasses = col_classes)
head(nyc_sample_df)

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
input_csv <- 'yellow_tripdata_2016-01.csv' # we can only use one month's data unless we join the CSVs
nyc_csv <- RxTextData(input_csv, colClasses = col_classes) # point to CSV file and provide column info
system.time(
  rxsum_csv <- rxSummary( ~ fare_amount, nyc_csv) # provide statistical summaries for fare amount
)
rxsum_csv