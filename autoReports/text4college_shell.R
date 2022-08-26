### NOTE: change the year in the Rmd file! ###

# install and load required packages
if (!require("pacman")) install.packages("pacman")
if (!require("Cairo")) install.packages("Cairo")
pacman::p_load(tidyverse, rmarkdown, ggplot2, ggpubr, ggthemes, showtext, ggrepel, DataCombine, readxl, Cairo)

# setwd('./Documents/Github/automated-reports/autoReportsText4College/')
# here are all the state files in the data folder
files <- list.files('./Data/')

# iterate through each row in each state file
states <- c('Alabama', 'Kentucky')
#states <- c('Kentucky')

for (state in states) {
  file <- grep(state, files, value = T)
  df <- read_excel(paste0('Data/', file), sheet=1)
  dir.create(file.path('Reports',state), showWarnings = FALSE)
  for (i in 1:nrow(df)) {
  # for (i in 1) {
    school_row <- df[i,]
    school_name <- school_row[['HS_Name']]

    name_list <- tools::toTitleCase(tolower(unlist(str_split(school_name, '_'))))
    name_list <- gsub('Ib', 'IB', name_list)
    name_list <- gsub('Hs', 'HS', name_list)
    name_list <- gsub('Of', 'of', name_list)
    name_list <- gsub('And ', 'and ', name_list)
    name_list <- gsub('The', 'the', name_list)
    
    school_name <- paste(name_list, collapse = ' ')
    school_name <- gsub('^the', 'The', school_name)
    school_name <- gsub('^andal', 'Andal', school_name)
    school_name <- gsub(' d ', ' D ', school_name)
    school_name <- gsub('Mcadory', 'McAdory', school_name)
    school_name <- gsub(' w ', ' W ', school_name)
    school_name <- gsub("#", "", school_name)
    print(school_name)
    
    if (grepl('/',school_name)) {
      school_name_pdf <- gsub('/', '-', school_name)
    } else {
      school_name_pdf <- school_name
    }
    
    rmarkdown::render("text4college.Rmd", 
                      clean = F,
                      params = list(state_abbr = state, school_row = school_row, school_name = school_name),
                      output_file = paste0("./Reports/", state, "/", school_name_pdf, ".pdf"))
    }
  }
