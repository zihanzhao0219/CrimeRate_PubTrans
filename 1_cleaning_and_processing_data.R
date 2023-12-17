library(dplyr)
library(lubridate)
library(tidyr)

# parallel computing
library(multidplyr)
library(parallel)
library(parallelly)


library(stringr)
library(ggplot2)
library(DescTools)
library(statip)

library(stargazer)
library(lfe)

library(MatchIt)
library(PSweight)
library(ipw)
library(lmtest)
library(misty)


library(beepr) # set up sound notification after task
library(microbenchmark)

options(max.print = 150)

# set up working directories -------------------

wd  <- list()
wd$pc <- '"C:/Users/Zihan/Desktop/mkt681_project"'

# 
# setwd(wd$mac)
# Sys.setenv('R_MAX_VSIZE'=32000000000)
# my_cores <- 8

setwd(wd$pc)
my_cores <- 8

wd$data <- paste0(getwd(), '/data/')
wd$code <- paste0(getwd(), '/code/')


# import data -------------------------------------------------------------

# Load necessary library
library(readr)

# List all CSV files in the folder
csv_files <- list.files('C:/Users/Zihan/Desktop/mkt681_project/data', pattern = "\\.csv$", full.names = TRUE)

# Function to create a valid R variable name from a file name
make_var_name <- function(file_path) {
  file_name <- basename(file_path)
  gsub("\\.csv$", "", file_name)  # Remove the .csv extension
}

# Read each CSV file into a sep arate data frame
for(file_path in csv_files) {
  data_frame_name <- make_var_name(file_path)
  assign(data_frame_name, read_csv(file_path))
}


ward_zip_2013 <- ward_zip_2013 %>% mutate(ward = as.numeric(ward),
                                          zip= as.numeric(zip))
ward_zip_2015 <- ward_zip_2015 %>% mutate(ward = as.numeric(ward),
                                          zip= as.numeric(zip))

# getting the y and t and z ----------------------------------------------

# get a list of zipcode x date

zipcode <- Zip_Codes_Boundaries %>% select(ZIP) %>% unique()

write.csv(zipcode,'unique_zipcode.csv',row.names = FALSE)


date_trip <- divvy_trips_1417 %>% select(`START TIME`) %>% 
  mutate(trip_date = substr(`START TIME`, 1, 10) ) %>% select(trip_date) %>% unique() %>%
  mutate(trip_date = as.Date(trip_date, format = "%m/%d/%Y"))


date_crime <- crimes_1417 %>% select(`Date`) %>% 
  mutate(crime_date = substr(`Date`, 1, 10) ) %>% select(crime_date) %>% unique() %>%
  mutate(crime_date = as.Date(crime_date, format = "%m/%d/%Y"))

start_date <- as.Date("2014-01-01")
end_date <- as.Date("2017-02-01")

date_sequence <- seq(from = start_date, to = end_date, by = "day")

df <- data.frame(date = date_sequence)
df$date_crime <- date_crime$crime_date

# everyday has crime but not necessary trips


relevant_stations <- divvy_trips_1417 %>% select(`FROM STATION ID`) %>% unique()
relevant_stations <- relevant_stations %>% left_join(Divvy_Bicycle_Stations, by = c(`FROM STATION ID`='ID'))
write.csv(relevant_stations,'relevant_stations.csv',row.names = FALSE)

all_zips <- station_info %>% select(ZIP) %>% unique() %>% na.omit()



# the y: the crime count per capita
crimes_1417 <- crimes_1417 %>% left_join(ward_zip_2013, by = c( 'Ward' = 'ward'))
crimes_1417 <-  crimes_1417 %>%  left_join(ward_zip_2015, by = c( 'Ward' = 'ward'))

crimes_1417 <- crimes_1417 %>% mutate(zip_code = if_else(Year < 2015, zip.x, zip.y))

df_y <- crimes_1417 %>% group_by(date,zip_code) %>% summarise(crime_count = n())

# the t: the trip counts per capita

divvy_trips_1417 <- divvy_trips_1417 %>% mutate(date = substr(`START TIME`, 1, 10) ) %>%
  mutate(date = as.Date(date, format = "%m/%d/%Y"))

df_t <- divvy_trips_1417 %>%
  left_join(station_info, by = c('FROM STATION ID' = 'FROM.STATION.ID'))  %>% 
  group_by(date,ZIP) %>% summarise(trip_count = n())


# the z: is the area socially disadvantaged 
zip_sda_coverage <- zip_sda_coverage %>% mutate(is_sda = if_else(percentage_coverage > 0.3,1,0))



# constructing the data------------------------------------------------------------------------
zip_date_mesh_0 <- crossing(ZIP = all_zips$ZIP, Date = date_crime$crime_date)

zip_date_mesh <- zip_date_mesh_0 %>% left_join(df_y, by = c('ZIP' = 'zip_code','Date' = 'date')) %>%
  mutate(crime_count = ifelse(is.na(crime_count),0,crime_count))
zip_date_mesh <- zip_date_mesh %>% left_join(df_t, by = c('ZIP','Date' = 'date')) %>%
  mutate(trip_count = ifelse(is.na(trip_count),0,trip_count))

zip_date_mesh$is_sda <- ifelse(zip_date_mesh$ZIP %in% zip_sda_coverage$ZIP, 1, 0)
zip_date_mesh$Z <- ifelse(zip_date_mesh$is_sda == 1 & zip_date_mesh$Date > as.Date('2015-07-07'), 1, 0)


zip_pop <- Chicago_Population_Counts %>% select(`Population - Total`,Year,Geography) %>%
  group_by(Geography) %>% summarise(mean_pop = mean(`Population - Total`, na.rm=TRUE))

zip_date_mesh <- zip_date_mesh %>% mutate(ZIP = as.character(ZIP))%>% left_join(zip_pop, by = c('ZIP' = 'Geography'))

viod_zip <- zip_date_mesh %>% filter(is.na(mean_pop)) %>% select(ZIP) %>% unique() 

zip_date_mesh  <- zip_date_mesh %>% filter(!ZIP %in% viod_zip$ZIP) %>% select(!c('is_sda'))

zip_date_mesh <- zip_date_mesh %>% mutate(crime_count  = crime_count  / mean_pop * 1000,
                                          trip_count  = trip_count  / mean_pop * 1000,
)

# write.csv(zip_date_mesh,'zip_date_mesh.csv',row.names = FALSE)

# adding controls------------------------------------------------------------------------

# ZIPCODE demographics
Chicago_Population_Counts$Male_Percentage <- (Chicago_Population_Counts$`Population - Male` / Chicago_Population_Counts$`Population - Total`) * 100
Chicago_Population_Counts$Asian_NonLatinx_Percentage <- (Chicago_Population_Counts$`Population - Asian Non-Latinx` / Chicago_Population_Counts$`Population - Total`) * 100
Chicago_Population_Counts$Black_NonLatinx_Percentage <- (Chicago_Population_Counts$`Population - Black Non-Latinx` / Chicago_Population_Counts$`Population - Total`) * 100
Chicago_Population_Counts$White_NonLatinx_Percentage <- (Chicago_Population_Counts$`Population - White Non-Latinx` / Chicago_Population_Counts$`Population - Total`) * 100

average_dem_data <- Chicago_Population_Counts %>%
  group_by(Geography) %>%
  summarise(Avg_Male_Percentage = mean(Male_Percentage, na.rm = TRUE),
            Avg_Asian_NonLatinx_Percentage = mean(Asian_NonLatinx_Percentage, na.rm = TRUE),
            Avg_Black_NonLatinx_Percentage = mean(Black_NonLatinx_Percentage, na.rm = TRUE),
            Avg_White_NonLatinx_Percentage = mean(White_NonLatinx_Percentage, na.rm = TRUE)
  )

zip_date_mesh <- zip_date_mesh %>% left_join(average_dem_data, by = c('ZIP' = 'Geography' ))

#  ZIPCODE Affordable_Rental_Housing_Developments；

aff_hous <- Affordable_Rental_Housing_Developments %>% group_by(`Zip Code` ) %>%
  summarise(n_aff_hous = sum(Units)) %>%
  mutate(`Zip Code` = as.character(`Zip Code`))
zip_date_mesh <- zip_date_mesh %>% left_join(aff_hous, by = c('ZIP' = 'Zip Code' ))


#  ZIPCODE 警局数量
n_police_station <- Police_Stations %>% group_by(ZIP ) %>%
  summarise(n_police_station = n()) %>%
  mutate(ZIP  = as.character(ZIP ))
zip_date_mesh <- zip_date_mesh %>% left_join(n_police_station, by = c('ZIP' = 'ZIP' ))

#  ZIPCODE bike station info
ttl_dock <- station_info %>% group_by(ZIP) %>% summarise(n_docks= sum(Total.Docks))%>%
  mutate(ZIP  = as.character(ZIP ))
zip_date_mesh <- zip_date_mesh %>% left_join(ttl_dock, by = c('ZIP' = 'ZIP' ))


# ZIPCODE；医院数量
n_clinic <- Chicago_Department_of_Public_Health_Clinic_Locations %>%  group_by(ZIP) %>%
  summarise(n_clinic = n()) %>%
  mutate(ZIP  = as.character(ZIP ))
zip_date_mesh <- zip_date_mesh %>% left_join(n_clinic, by = c('ZIP' = 'ZIP' ))



# ZIPCODE X YEAR: 当年新开的business数量
n_businss_license <- Business_Licenses %>% filter(`APPLICATION TYPE` == 'ISSUE') %>% 
  mutate(year = substr(`APPLICATION CREATED DATE`, nchar(`APPLICATION CREATED DATE`) - 3, nchar(`APPLICATION CREATED DATE`))) %>%
  group_by(`ZIP CODE`, year) %>%
  summarise(n_businss_license = n()) %>%
  mutate(`ZIP CODE`  = as.character(`ZIP CODE` )) 

zip_date_mesh <- zip_date_mesh %>% mutate(year = format(Date, "%Y")) %>% 
  left_join(n_businss_license, by = c('ZIP' = 'ZIP CODE', 'year')) 

# ZIPCODE 老人院数量；
n_senior_center <- Senior_Centers %>% group_by(ZIP) %>%
  summarise(n_senior_center = n()) %>%
  mutate(ZIP  = as.character(ZIP)) 

zip_date_mesh <- zip_date_mesh %>% left_join(n_senior_center, by = c('ZIP' = 'ZIP')) 

# ZIPCODE x DATE: Micro-Market_Recovery_Program_-_Violations_and_Inspections
df_tmp <- `Micro-Market_Recovery_Program_-_Violations_and_Inspections`
n_MMRP_violation <- df_tmp %>% mutate(vio_time = substr(`Violation Date`, 1, 10)) %>% 
  filter(!is.na(vio_time)) %>%
  group_by(`Zip Code`,vio_time) %>% summarise(n_violation = n()) %>%
  mutate(vio_time =  as.Date(vio_time, format = "%d/%m/%Y"),
         vio_time = format(vio_time, "%Y-%m-%d"),
         `Zip Code`  = as.character(`Zip Code` )
  )


n_MMRP_inspection <- df_tmp %>% mutate(ins_time = substr(`Inspection Completed Date and Time`, 1, 10)) %>% 
  filter(!is.na(ins_time)) %>%
  group_by(`Zip Code`,ins_time) %>% summarise(n_inspection = n())%>%
  mutate(ins_time =  as.Date(ins_time, format = "%d/%m/%Y"),
         ins_time = format(ins_time, "%Y-%m-%d"),
         `Zip Code`  = as.character(`Zip Code` )
  )

zip_date_mesh <- zip_date_mesh %>% mutate(Date = as.character(as.Date(Date))) %>% left_join(n_MMRP_violation, by = c('ZIP' = 'Zip Code', 'Date' = 'vio_time')) 
zip_date_mesh <- zip_date_mesh %>% mutate(Date = as.character(as.Date(Date))) %>% left_join(n_MMRP_inspection, by = c('ZIP' = 'Zip Code','Date' = 'ins_time')) 


# ZIP X DATE Financial_Incentive_Projects_-_Small_Business_Improvement_Fund__SBIF_
sbif <- read.csv('data/Financial_Incentive_Projects_-_Small_Business_Improvement_Fund__SBIF_.csv')

sbif <- sbif %>%
  filter(!is.na(APPROVAL.DATE)) %>%
  mutate(APPROVAL.DATE = as.Date(APPROVAL.DATE, format = "%m/%d/%Y")) %>%
  filter(APPROVAL.DATE < as.Date('2017-02-01'),APPROVAL.DATE >= as.Date('2014-01-01')) 

sbif <- sbif %>% left_join(ward_zip_2013, by = c('WARD'='ward'))

amount_sbif <- sbif %>% group_by(zip,APPROVAL.DATE) %>% summarise(amount_sbif = sum(INCENTIVE.AMOUNT)) %>%
  mutate( zip  = as.character(zip ),APPROVAL.DATE = as.character(APPROVAL.DATE))

zip_date_mesh <- zip_date_mesh %>% left_join(amount_sbif, by = c('ZIP' = 'zip','Date' = 'APPROVAL.DATE')) 

# 
# # ZIPCODE: 用电量
# df_tmp <- Average_Electricity_Usage_per_Square_Foot_by_Community_Area
# colnames(df_tmp)
# 
# # Standardize the case of the name column (convert to lower case) in both data frames
# CommAreas <- CommAreas %>%
#   mutate(COMMUNITY = tolower(COMMUNITY))
# 
# df_tmp <- df_tmp %>%
#   mutate(`COMMUNITY AREA NAME` = tolower(`COMMUNITY AREA NAME`))
# 
# df_tmp <- df_tmp %>% left_join(CommAreas, by = c('COMMUNITY AREA NAME' = 'COMMUNITY'))
# 

zip_date_mesh <- zip_date_mesh %>%  mutate_all(~ifelse(is.na(.), 0, .))

write.csv(zip_date_mesh,'zip_date_mesh.csv',row.names = FALSE)

