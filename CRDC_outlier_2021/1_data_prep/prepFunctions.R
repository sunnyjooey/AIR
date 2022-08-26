#packages
library(dplR)

# Helper functions for wrangling 
specialVars <- function(crdc){
  # character cols
  chars <- names(sapply(crdc, class)[sapply(crdc, class) == 'character'])
  crdc[ , chars] <- sapply(chars, function(col) tolower(crdc[[col]]))
  # Reformat Variables 
  ## SCH_ALTFOCUS
  crdc$sch_altfocus[crdc$sch_altfocus == "."] <- 0
  crdc$sch_altfocus[crdc$sch_altfocus == "academic"] <- 1
  crdc$sch_altfocus[crdc$sch_altfocus == "discipline"] <- 2
  crdc$sch_altfocus[crdc$sch_altfocus == "both"] <- 3
  crdc$sch_altfocus <- as.numeric(crdc$sch_altfocus) 
  crdc$sch_altfocus_pre_mean <- ifelse(crdc$sch_altfocus == 1 | crdc$sch_altfocus == 3, 1, 0)
  crdc$sch_altfocus_post_mean <- ifelse(crdc$sch_altfocus == 2 | crdc$sch_altfocus == 3, 1, 0)
  ## SCH_JJTYPE  
  crdc$sch_jjtype[crdc$sch_jjtype == "."] <- 0
  crdc$sch_jjtype[crdc$sch_jjtype == "pre"] <- 1
  crdc$sch_jjtype[crdc$sch_jjtype == "post"] <- 2
  crdc$sch_jjtype[crdc$sch_jjtype == "both" ] <- 3
  crdc$sch_jjtype <- as.numeric(crdc$sch_jjtype) 
  crdc$sch_jjtype_pre_mean <- ifelse(crdc$sch_jjtype == 1 | crdc$sch_jjtype == 3, 1, 0)
  crdc$sch_jjtype_post_mean <- ifelse(crdc$sch_jjtype == 2 | crdc$sch_jjtype == 3, 1, 0)
  ## yes_no
  crdc[crdc=='yes'] <- 1
  crdc[crdc=='y'] <- 1
  crdc[crdc=='no'] <- 0
  crdc[crdc=='n'] <- 0
  ## covid-related instruction types
  # answer "d" is no change, so it is the base case
  crdc$sch_dind_instructiontype_inperson <- ifelse(crdc$sch_dind_instructiontype == "a", 1, 0)
  crdc$sch_dind_instructiontype_virtual <- ifelse(crdc$sch_dind_instructiontype == "b", 1, 0)
  crdc$sch_dind_instructiontype_hybrid <- ifelse(crdc$sch_dind_instructiontype == "c", 1, 0)
  crdc$sch_dind_instructiontype_ns <- ifelse(crdc$sch_dind_instructiontype == "ns", 1, 0) 
  crdc$sch_dind_virtualtype_in <- ifelse(crdc$sch_dind_virtualtype == "a" | crdc$sch_dind_virtualtype == "c", 1, 0)
  crdc$sch_dind_virtualtype_out <- ifelse(crdc$sch_dind_virtualtype == "b" | crdc$sch_dind_virtualtype == "c", 1, 0)
  ## drop
  crdc$sch_jjtype <- NULL
  crdc$sch_altfocus <- NULL
  crdc$sch_dind_instructiontype <- NULL
  crdc$sch_dind_virtualtype <- NULL
  
  ### Other General Formatting 
  for (col in colnames(crdc)){
    if (col %in% chars) {
      crdc[crdc[[col]] %in% c('ns'), col] <- NA
    } else {
      # these are converted dummies that need to be numeric
      crdc[[col]] <- as.numeric(crdc[[col]])
      crdc[is.na(crdc[[col]]), col] <- 0
    }
  }
  crdc[is.na(crdc)] <- 0
  crdc$leaid <- tolower(crdc$leaid)
  crdc$schid <- tolower(crdc$schid)
  ## drop grade variables 
  # grades <- grep("sch_grade_",colnames(crdc), value = TRUE)
  # ugdetail <- grep("sch_ugdetail_",colnames(crdc), value = TRUE)
  # crdc <- crdc[,!colnames(crdc) %in% c(grades,ugdetail)]
  return(crdc)
}


combineRace <- function(crdc){
  ret <- list()
  # Combining non-white race varaibles
  # get race_vars
  race_vars <- grep(".*_(hi|am|as|hp|bl|tr)_(m|f)",colnames(crdc), value = TRUE)
  stems_oth <- c("_hi", "_am", "_as","_hp", "_bl", "_tr")
  # get root  
  for(stem in stems_oth){
    race_vars <- gsub(stem,"",race_vars)
  }
  race_vars <- unique(race_vars) 
  # combine 
  for(var in race_vars){
    newName <- paste0(substr(var,1,nchar(var)-2), "_ot", substr(var,nchar(var)-1,nchar(var)))
    oldNames <- paste0(substr(var,1,nchar(var)-2), stems_oth, substr(var,nchar(var)-1,nchar(var)))
    crdc[,oldNames] <- sapply(oldNames, function(col) as.numeric(crdc[[col]]))
    crdc[,newName] <- rowSums(crdc[,oldNames])
    crdc[,oldNames] <- NULL 
  }
  ret$vars <- race_vars
  ret$dat <- crdc
  return(ret)
}


getTotals <- function(crdc){
  # total
  crdc$tot_enrl <- rowSums(crdc[,c("sch_enr_ot_m","sch_enr_wh_m",
                                   "sch_enr_ot_f","sch_enr_wh_f")])
  # disabilities 
  crdc$sch_504enr <- rowSums(crdc[,c("sch_504enr_ot_m","sch_504enr_wh_m",
                                     "sch_504enr_ot_f","sch_504enr_wh_f")])
  crdc$sch_ideaenr <- rowSums(crdc[,c("sch_ideaenr_ot_m","sch_ideaenr_wh_m",
                                      "sch_ideaenr_ot_f","sch_ideaenr_wh_f")])
  crdc$sch_lepenr <- rowSums(crdc[,c("sch_lepenr_ot_m","sch_lepenr_wh_m",
                                     "sch_lepenr_ot_f","sch_lepenr_wh_f")])
  # male and female enrollment ratios
  crdc$sch_enr_m <- rowSums(crdc[,c("sch_enr_ot_m","sch_enr_wh_m")])
  crdc$sch_enr_f <- rowSums(crdc[,c("sch_enr_ot_f","sch_enr_wh_f")])
  
  return(crdc)
}


normalizeCRDC <- function(crdc){
  # male female ratio
  crdc$mf_ratio <- crdc$sch_enr_f / crdc$tot_enrl 
  crdc$mf_ratio[is.na(crdc$mf_ratio)] <- 0 
  
  # special enrollment ratios
  crdc$enr504_ratio <- crdc$sch_504enr / crdc$tot_enrl
  crdc$enr504_ratio[is.na(crdc$enr504_ratio)] <- 0 
  crdc$enridea_ratio <- crdc$sch_ideaenr / crdc$tot_enrl
  crdc$enridea_ratio[is.na(crdc$enridea_ratio)] <- 0 
  crdc$enrlep_ratio <- crdc$sch_lepenr / crdc$tot_enrl
  crdc$enrlep_ratio[is.na(crdc$enrlep_ratio)] <- 0 
  crdc[crdc$enrlep_ratio > 1, 'enrlep_ratio'] <- 1 
  
  return(crdc)
}


#tots <- c("tot_enrl","sch_504enr","sch_ideaenr","sch_lepenr","sch_enr_m","sch_enr_f","mf_ratio")

