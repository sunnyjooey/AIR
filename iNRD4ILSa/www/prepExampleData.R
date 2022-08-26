# Packages
library(tidyverse)
library(EdSurvey)

setwd("C:/Users/mlee/Documents/GitHub/iNRD4ILSa")

# download TIMMS 
# downloadTIMSS("Q:/Eric/iNRDTimss", c(2011, 2015))

## changed up path
if(Sys.info()[[1]] == "Windows") {
	T11Path <- "//dc2fs/dc4work/ESSIN Task 14/NAEP R Program/International Assessment/ShinyApp/Data/SelectedSmallScallTIMSSData_2011"
	T15Path <- "//dc2fs/dc4work/ESSIN Task 14/NAEP R Program/International Assessment/ShinyApp/Data/SelectedSmallScallTIMSSData_2015"
} else {
	T11Path <- "/Volumes/NAEP R Program/International Assessment/ShinyApp/Data/SelectedSmallScallTIMSSData_2011"
	T15Path <- "/Volumes/NAEP R Program/International Assessment/ShinyApp/Data/SelectedSmallScallTIMSSData_2015"	
}

# read frames 
# 11 
T11_G4 <- readTIMSS(path = T11Path, c("*"), "4")
T11_G8 <- readTIMSS(path = T11Path, c("*"), "8")
# 15 
T15_G4 <- readTIMSS(path = T15Path, c("*"), "4")
T15_G8 <- readTIMSS(path = T15Path, c("*"), "8")

# combine into one object and save for use 
timssSample <- list(T11_G4 = T11_G4,T11_G8 = T11_G8, T15_G4 = T15_G4, T15_G8 = T15_G8) 
# class(timssSample) <- c("list", "edsurvey.data.frame.list")
if(Sys.info()[[1]] == "Windows") {
	saveRDS(timssSample, paste0(getwd(),"/data/timmsSample3_windows.rds"))
} else {
	saveRDS(timssSample, paste0(getwd(),"/data/timmsSample3.rds"))
}

