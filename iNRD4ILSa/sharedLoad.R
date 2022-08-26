
suppressPackageStartupMessages({
  library(shiny)
  library(shinythemes)
  library(shinydashboard)
  library(ggplot2)
  library(grid)
  library(DT)
  
  #database and serialization packages
  library(DBI)
  library(RMariaDB) #supercedes RMySQL package
  library(jsonlite)
  library(config)
  library(pool)
  
  library(magrittr)
  library(tidyverse)
  library(reshape2)
  library(scales)
  library(shinyWidgets)
  library(shinyjs)
  library(showtext)
  library(svglite)
  library(tools)
  # library(ggrepel)
  # library(plotly)
  # library(ggthemes)
  # library(colorspace)
  # library(RColorBrewer)
  library(devtools)
  library(EdSurvey)
  # library(shinyTree)
  library(readxl)
  library(shinycssloaders)
  library(bsplus)
  library(readr)
  library(sortable)
})

#primary data object.  list of edsurvey.data.frame.list objects in format.
#names: T11_G4, T11_G8, T15_G4, T15_G8
shinyILSAData <- getShinyILSAData.TIMSS()

#ensure DB connection pool can be established and pool is created
connResult <- getDBPoolConnection() # #in dataCacheDB.R file

DB_CACHE_ACTIVE <- connResult$enableDBCaching
if(DB_CACHE_ACTIVE){
  
  dbConnectPool <- connResult$dbConnectPool
  
  #once shiny is stopped we need to ensure connection pool is not leaked
  onStop(function() {
    poolClose(dbConnectPool)
  })
}
connResult <- NULL

country_cats <- read_xlsx("data/criteriaSelection.xlsx")
# load the fonts
path <- file.path(getwd()) # some issues with sysfonts
tryCatch({
  font_add("Open Sans", regular = paste0(path,"/fonts/OpenSans-Regular.ttf"))
  font_add("Open Sans Italic", regular = paste0(path,"/fonts/OpenSans-Italic.ttf"))
  font_add("Open Sans Semibold", regular = paste0(path,"/fonts/OpenSans-Semibold.ttf"))
  font_add("Open Sans Semibold Italic", regular = paste0(path,"/fonts/OpenSans-SemiboldItalic.ttf"))
  }, error = function(cond) {
    message(cond)
})
showtext_auto()

# preprocessed accordion data (to avoid generating then on the fly which is slow)
# preprocessedAccordion_G4_2011_2015 <- readRDS("data/sample.RDS")
# preprocessedAccordion_G4_2011 <- readRDS("data/sample.RDS")
# preprocessedAccordion_G4_2015 <- readRDS("data/sample.RDS")
# preprocessedAccordion_G8_2011_2015 <- readRDS("data/sample.RDS")
# preprocessedAccordion_G8_2011 <- readRDS("data/sample.RDS")
# preprocessedAccordion_G8_2015 <- readRDS("data/sample.RDS")
