# Compile panel data set of indicators from:
    # Legatum Institute, Legatum Prosperity Index
    # Heritage Foundation, Index of Economic Freedom
    # World Bank, World Development Indicators (WDI)
    # BEA, United States FDI


# Set up libraries and paths --------------------------------------------------------

library(haven)
library(tidyverse)
library(plyr)
library(dplyr)
library(readxl)


# Change to your local drive
drive <- "S:\\"
ESDB  <- sprintf("%sESDB\\Database\\", drive)
Req   <- sprintf("%sEADS Requests\\2020_03_31 Michael Nicholson - Africa Poverty Research\\", drive)

# Import files -------------------------------------------------------------
# Get master ESDB files
series <- read_sas(sprintf("%sseries.sas7bdat", ESDB))
sources <- read_sas(sprintf("%ssources.sas7bdat", ESDB))

# Series and Country List
series_list <- read_xlsx(sprintf("%sEdited data\\series_list.xlsx", Req))
countries_list <- read_xlsx(sprintf("%sEdited data\\countries_list.xlsx", Req))


# Import ESDB Data --------------------------------------------------------

# Turn source_ids into sas dataset names
series_list$file <- ifelse(series_list$source_id <100, 
                       str_c(ESDB,"_0",series_list$source_id,"data.sas7bdat"), 
                       str_c(ESDB,"_",series_list$source_id,"data.sas7bdat"))

# Create lists of sources and series that we need
esdb_sources_needed = unique(series_list$file)
esdb_series_needed = series_list$series_id

# Create a function that will pull the data for the series and sources you need from the ESDB 
# This function has three inputs:
  # x - sources
  # s - series
  # y - years
# It will look through the sas datasets and filter each for the series and years that you need.
pull_ESDB <- function(x, s, y){
  read_sas(x) %>%
    filter(series_id %in% s & year >=y) %>%
    select(country_id, series_id, year, value_start)
}

# Use the lists to pull ESDB data. The following will apply the list 'esdb_sources_needed' to the pull_ESDB function
# and return all of the datasets together as a dataframe 'ctd_esdb'
data_esdb <- ldply(.fun = pull_ESDB, .data = esdb_sources_needed, esdb_series_needed, 2009, .progress = progress_text())


# Format as panel data set ------------------------------------------------

data1 <- data_esdb %>% 
  left_join(countries_list, by = "country_id") %>% # map in the countries wanted
  filter(country_name != "drop") %>%  # remove the extra countries
  left_join(series_list, by = "series_id") %>% # join in the series names
  select(country_id, country_name, code, year, value_start) %>% # drop unneccessary columns 
  spread(code, value_start, fill = NA) %>%  # spread it to wide format
  mutate(fdi = ifelse(fdi == 88888888, NA, fdi),
         ln_gdp = log(gdp),  # take the natural log of these indicators
         ln_fdi = log(fdi))

write_csv(data1, sprintf("%sFinal data\\individual_african_countries.csv", Req), na = "")

# Compile Metadata --------------------------------------------------------

metadata <- series_list %>% 
  left_join(sources, by = "source_id") %>% 
  left_join(series, by = "series_id") %>% 
  select(code, series_name.x, source_name, source_url, series_definition)

write_csv(metadata, sprintf("%sFinal data\\metadata.csv", Req), na = "")
