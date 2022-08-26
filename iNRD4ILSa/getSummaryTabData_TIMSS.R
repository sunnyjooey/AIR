#calculates the summary statistics for a an outcome variable and grouping variable(s) using the edsurveyTable EdSurvey function
#returns a formatted data.frame with all the variables necessary for the application

TIMSS_summarytab_MagicNumber <- 2

getSummaryTabData.TIMSS <- function(shinyILSAData, codebook, selectedVars, grade, years, countries, outcomeVarDesc, groupingVarsDesc, weightVar = weightVar){
  
  #convert grade from "Grade 4", "Grade 8" to numeric: 4 or 8
  gradeNum <- gsub("^grade","", grade, ignore.case = TRUE)
  gradeNum <- as.numeric(trimws(gradeNum))
  
  #ensure years and countries are sorted
  years <- sort(years)
  years <- as.character(years) #ensure it's a character
  
  countries <- sort(countries)
  
  #ensure weightVar is actually null value
  if(tolower(weightVar)=="null" || is.null(weightVar) || is.na(weightVar)){
    weightVar <- NULL #needs to be NULL for the edsurvey analysis call
  }
  
  #filter the codebook only to our selectedVars (they still might not be used in the call, but helps keep the codebook small)
  codebook <- subset(codebook, codebook$ID %in% selectedVars) #filter the values based on the selected variables by the user (using the ID)
  
  resultList <- vector("list", length = (length(years)*length(countries))) #list to store out either calculated or cached result items, allocate correct list size
  groupingVarsReconciled <- c()
  iResult <- 1 #index for appending items to our result list
  
  for(yr in years){
    
    #the actual variables names 
    formulaVarLHS <- codebook[codebook$Labels %in% outcomeVarDesc & codebook$grade==grade , paste0("variableName", yr)]
    formulaVarRHS <- c()
    for (var in groupingVarsDesc) {
      if(length(groupingVarsDesc) == 1 && groupingVarsDesc == 1) {
        formulaVarRHS <- 1
      } else {
        new_var <- codebook[codebook$Labels == var & codebook$grade==grade , paste0("variableName", yr)]
        formulaVarRHS <- c(formulaVarRHS, new_var)       
      }
    }
    # formulaVarRHS <- codebook[codebook$Labels %in% groupingVarsDesc & codebook$grade==grade , paste0("variableName", yr)]
    
    isValidFormula <- !anyNA(formulaVarLHS) && !anyNA(formulaVarRHS) #checks if the formula is valid by having the requested variable names for that year
    
    if(isValidFormula){
      formula <- paste0(formulaVarLHS, " ~ ", paste0(formulaVarRHS, collapse = " + "))
      
      groupingVarsReconciled <- formulaVarRHS
      
      formulaID_LHS <- codebook[codebook$Labels %in% outcomeVarDesc & codebook$grade==grade , "ID"]
      formulaID_RHS <- c()
      for (var in groupingVarsDesc) {
        new_var <- codebook[codebook$Labels == var & codebook$grade==grade , "ID"]
        formulaID_RHS <- c(formulaID_RHS, new_var)
      }
      # formulaID_RHS <- codebook[codebook$Labels %in% groupingVarsDesc & codebook$grade==grade , "ID"]
      formulaID <- paste0(formulaID_LHS, " ~ ", paste0(formulaID_RHS, collapse = " + "))
      
    }
    
    #This is the 'code' we used for naming the EdSurvey.Data.Frame.Lists (e.g., 'T15_G4' == TIMSS 2015 Grade 4, 'T11_G8' == TIMSS 2011 Grade 8) 
    #see 'buildShinyILSA_DataList.TIMSS' in the dataController_TIMSS.R file for how the 'shinyILSAData' object is created
    listCode <- paste0("T", substr(yr, 3,4), "_", "G", gradeNum)
    iList <- which(names(shinyILSAData)==listCode) #get the list index to our specific year and grade
    
    #check we have only one list selected here
    if(length(iList)<1){
      stop(paste0("Unable to locate edsurvey.data.frame.list with name: ", listCode))
    }else if(length(iList)>1){
      stop(paste0("Found multiple edsurvey.data.frame.list with name: ", listCode))
    }
    
    esdfl <- shinyILSAData[[iList]] #grab the edsurvey.data.frame list we require
    
    #flag which countries have actual entries in the edsurvey.data.frame.list and which do not for this year
    hasCountry <- countries %in% esdfl$covs$country

    #====Loop Through Each Country 
    for(iCntry in seq_along(countries)){
      
      cntry <- countries[iCntry]
      
      if(hasCountry[iCntry] && isValidFormula){ #has country entry and is a valid formula
        
        if(DB_CACHE_ACTIVE && hasCrossTabResult_Cached.TIMSS(formulaID = formulaID,
                                                              weightVar = weightVar,
                                                              grade = grade, 
                                                              year = yr,
                                                              country = cntry)){ #check if result is available
          
          resultList[[iResult]] <- getCrossTabResult_Cached.TIMSS(formulaID = formulaID,
                                                                  weightVar = weightVar,
                                                                  grade = grade,
                                                                  year = yr,
                                                                   country = cntry)
        }else{ #calculate
          
          #determine the index position as they might be different
          lookupIdx <- which(esdfl$covs$country==cntry)
          esdf <- esdfl$datalist[[lookupIdx]] #get the edsurvey.data.frame object needed for the analytic call
          
          resultList[[iResult]] <- getCrossTabResult_Calc.TIMSS(formulaID =  formulaID, 
                                                                formulaCall = formula,
                                                                esdf =  esdf, 
                                                                weightVar = weightVar,
                                                                grade = grade,
                                                                year = yr,
                                                                country = cntry)
        }
      }else{ #no edsurvey.data.frame for year/country OR invalid formula call for the year. just return an item with NULL result item
        resultList[[iResult]] <- crosstabResultItem.TIMSS(formulaID = NA,
                                                          formulaCall = NA,
                                                          grade = grade,
                                                          year = yr,
                                                          country = cntry,
                                                          weight = weightVar,
                                                          result = NULL)
      }#end if(hasCountry[iCntry] && isValidFormula)
      
      iResult <- iResult + 1
    }#end for(iCntry in cntryIdx)
    
  }#end for(yr in years)
  
  #format our result list for returning back
  resultDF <- formatCrossTabResultList.TIMSS(resultList = resultList, 
                                             formulaCall = formulaCall,
                                             groupingVarNames = groupingVarsReconciled,
                                             weightVar = weightVar)
  return(resultDF)
}

#make the EdSurvey summary2 call and returns a 'summary.result.TIMSS' of the call.
getCrossTabResult_Calc.TIMSS <- function(formulaID, formulaCall, esdf, weightVar, grade, year, country){
  
  res <- NULL
  
  processArgs <- list(formula = Formula::as.Formula(formulaCall),
                      data = esdf,
                      weightVar = weightVar,
                      jrrIMax = Inf,
                      varMethod = "jackknife")
  
  success <- tryCatch({
    res <- do.call("edsurveyTable", args = processArgs, quote = FALSE)
    
    TRUE
  }, error = function(e){
    message(paste0("Error in edsurveyTable Calc! Country:", country, "|Year:", year, "|Grade:", grade, "|Message: ", e$message))
    FALSE
  })
  
  if(success && !is.null(res)){
    
    #built the result item list for the summary.result that includes all of the information regarding this
    resItem <- crosstabResultItem.TIMSS(formulaID = formulaID,
                                        formulaCall = formulaCall,
                                        grade = grade,
                                        year = year,
                                        country = country,
                                        weight = weightVar,
                                        result = res)
    
    #cache this object to a blob for faster retrieval later
    #only cache if result is valid!
    cacheCrossTabResult.TIMSS(formulaID = formulaID,
                              formulaCall = formulaCall,
                              weightVar = weightVar,
                              grade = grade,
                              year = year,
                              country = country,
                              resultObj = resItem)
    
  }else{
    
    #built the result item list for the summary.result that includes all of the information regarding this
    resItem <- crosstabResultItem.TIMSS(formulaID = formulaID,
                                        formulaCall = formulaCall,
                                        grade = grade,
                                        year = year,
                                        country = country,
                                        weight = weightVar,
                                        result = NULL)
  }
  
  return(resItem)
}

#constructor for building a summary.result.TIMSS object to help keep things organized
crosstabResultItem.TIMSS <- function(formulaID, formulaCall, grade, year, country, weight, result = NULL){
  
  resItem <- list(formulaID = formulaID,
                  formulaCall = formulaCall,
                  grade = grade,
                  year = as.character(year),
                  country = country,
                  weight = weight,
                  result = result) #build our list object meeting our object specification
  
  class(resItem) <- "crosstab.result.TIMSS"
  return(resItem)
}

#this returns the result from the cached items for the exact call per grade/year/country
getCrossTabResult_Cached.TIMSS <- function(formulaID, weightVar, grade, year, country){
  
  resultSpec <- "SELECT resultObj FROM crosstab_result WHERE formulaID = ? AND 
                                                              grade = ? AND 
                                                              year = ? AND 
                                                              country = ? AND 
                                                              weight = ? AND
                                                              magic_number = ? AND
                                                              force_recalc = 0"
  
  paramList <- list(paste0(formulaID, collapse = "||"),
                    grade,
                    year,
                    country,
                    weightVar,
                    TIMSS_summarytab_MagicNumber)
  
  #unable to specify a NULL value as an unnamed parameter here
  if(is.null(weightVar)){
    resultSpec <- sub("weight = ?", "weight is null", resultSpec, fixed = TRUE)
    paramList[[5]] <- NULL #drop the weightVar from the parameters list
  }
  
  res <- dbGetQuery(dbConnectPool, statement = resultSpec, params = paramList)
  res <- res[1,1] #this a list here since that's what we passed during INSERT
  
  listObj <- jsonlite::unserializeJSON(res)
  listObj$year <- year #ensure it's same data type here, seems to do implicit conversion!
  
  return(listObj)
}

hasCrossTabResult_Cached.TIMSS <- function(formulaID, weightVar, grade, year, country){
  
  countSpec <- "SELECT Count(*) FROM crosstab_result WHERE formulaID = ? AND 
                                                            grade = ? AND 
                                                            year = ? AND 
                                                            country = ? AND 
                                                            weight = ? AND
                                                            magic_number = ? AND
                                                            force_recalc = 0"
  
  paramList <- list(formulaID,
                    grade,
                    year,
                    country,
                    weightVar,
                    TIMSS_summarytab_MagicNumber)
  
  #unable to specify a NULL value as an unnamed parameter here with dbGetQuery call
  if(is.null(weightVar)){
    countSpec <- sub("weight = ?", "weight is null", countSpec, fixed = TRUE)
    paramList[[5]] <- NULL #drop the weightVar from the parameters list
  }
  
  res <- dbGetQuery(dbConnectPool, statement = countSpec, params = paramList)
  
  return(res[1,1]>0) #return TRUE/FALSE based on the count.  The res object will only have 1 row and 1 column containing the count value
}

cacheCrossTabResult.TIMSS <- function(formulaID, formulaCall, weightVar, grade, year, country, resultObj){
  
  #if the cache is not active, then immediately return NULL here
  if(DB_CACHE_ACTIVE==FALSE){
    return(invisible(NULL))
  }
  
  esv <- paste(utils::packageVersion("EdSurvey"), sep=".")
  json <- jsonlite::serializeJSON(resultObj) #serialize this to json string format for use later
  
  insertSpec <- "INSERT INTO crosstab_result (formulaID, 
                                                formulaVar,
                                                grade, 
                                                year, 
                                                country, 
                                                weight, 
                                                resultObj,
                                                edsurvey_version,
                                                magic_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"
  
  #for handling summaries with multiple variables collapse them with double pipe delimiter
  formulaID <- paste0(formulaID, collapse = "||")
  formulaVar <- paste0(formulaCall, collapse = "||")
  
  weightVar <- ifelse(is.null(weightVar), NA, weightVar) #NA is stored as NULL in this instance
  
  paramList <- list(formulaID,
                    formulaVar,
                    grade,
                    year,
                    country,
                    weightVar,
                    json,
                    esv,
                    TIMSS_summarytab_MagicNumber)
  
  res <- dbExecute(dbConnectPool, statement = insertSpec, params = paramList)
  
  if(res!=1){
    warning(paste0("Cache Insert Failed!"))
  }
  
  return(invisible(NULL))
}

#format a list of 'summary.result.TIMSS' objects to return a properly formatted data.frame result
formatCrossTabResultList.TIMSS <- function(resultList, formulaCall, groupingVarNames, weightVar){
  
  isWeighted <- !is.null(weightVar) && weightVar!="NULL"
  
  summaryCols <- c(groupingVarNames, "N", "WTD_N", "PCT", "SE(PCT)", "MEAN", "SE(MEAN)")

  #these are appended fields that are not normally retured in an EdSurvey summary2 result
  if(isWeighted){
    appendCols <- c("Year", "Grade", "Country", "Weight")
  }else{
    appendCols <- c("Year", "Grade", "Country")
  }
  
  #creates a data.frame with all logical fields acting as our model
  modelDF <- data.frame(matrix(ncol= (length(summaryCols)+length(appendCols)), nrow = 0)) 
  names(modelDF) <- c(summaryCols, appendCols) #give the modelDF the correct names
  
  outputList <- list()
  iList <- 1
  for(item in resultList){
    
    if(!is.null(item$result)){ #ensure it has a result object
      tempDF <- item$result$data #this needs match the 'summaryCols' specification
      colnames(tempDF)[1:length(groupingVarNames)] <- groupingVarNames
    }else{ #NO DATA EXISTS OR HAD ERROR when running
      tempDF <- modelDF
      tempDF[1, ] <- NA
    }
    
    #add these in the order of the appendCols to match the 'model' df
    tempDF$Year <- item$year
    tempDF$Grade <- item$grade
    tempDF$Country <- item$country
    if(isWeighted){
      tempDF$Weight <- item$weight
    }
    
    outputList[[iList]] <- tempDF
    iList <- iList + 1
  }
  
  #stack the list into a single data.frame
  outDF <- suppressWarnings(dplyr::bind_rows(outputList))
  
  #format the data.frame for character output, including rounding, adding dashes for NA, etc.
  outDF[] <- lapply(outDF, function(x){
    if(is.integer(x)){
      x <- as.character(x)
    }else if(is.numeric(x)){
      x <- as.character(round(x,4)) #round to 4 decimal places for consistency
    }else{
      x <- as.character(x)
    }
    
    #lastly convert NA values to dashes
    x[is.na(x)] <- "-"
    x #return full col
  })
  
  #reorder columns for consistent output
  cn <- colnames(outDF)
  cn1 <- c("Grade", "Year", "Country") #first variables we will always have
  cn2 <- cn[cn %in% c(groupingVarNames)] #the formula variables or 'Variable' if categorical
  cn3 <- cn[!(cn %in% c(cn1, cn2, "Weight"))] #the rest of the columns as originally ordered, excluding grouping vars and weight
  cn4 <- cn[cn %in% c("Weight")] #this variable should be last
  outDF <- outDF[ , c(cn1, cn2, cn3, cn4)]
  
  return(outDF)
}