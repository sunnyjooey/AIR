#calculates the summary statistics for a variable, or variables, using the 'summary2' EdSurvey function
#returns a formatted data.frame with all the variables necessary for the shiny app

TIMSS_summary_MagicNumber <- 2

getDescriptiveData.TIMSS <- function(shinyILSAData, codebook, selectedVars, grade, years, countries, descriptiveVarDesc, weightVar = NULL){

  # dbCon <- dbConnectPool #connection pool created in sharedLoad.R, grab a reference of it here
  
  #convert grade from "Grade 4", "Grade 8" to numeric: 4 or 8
  gradeNum <- gsub("^grade","", grade, ignore.case = TRUE)
  gradeNum <- as.numeric(trimws(gradeNum))
  
  #ensure years and countries are sorted
  years <- sort(years)
  years <- as.character(years) #ensure years is character here to eliminate issues downstream for conversions
  
  countries <- sort(countries)
  
  #ensure weightVar is actually null value
  if(tolower(weightVar)=="null" || is.null(weightVar) || is.na(weightVar)){
    weightVar <- NULL #needs to be NULL for the edsurvey analysis call
  }
  
  #filter the codebook only to our selectedVars (they still might not be used in the call, but helps keep the codebook small)
  codebook <- subset(codebook, codebook$ID %in% selectedVars) #filter the values based on the selected variables by the user (using the ID)
  
  resultList <- vector("list", length = (length(years)*length(countries))) #list to store out either calculated or cached result items, allocate correct list size
  varNamesReconciled <- c()
  
  iResult <- 1 #index for appending items to our result list
  
  for(yr in years){
    
    formulaVar <- codebook[codebook$Labels %in% descriptiveVarDesc & codebook$grade==grade , paste0("variableName", yr)]
    isValidFormula <- !anyNA(formulaVar)
    
    if(isValidFormula){
      varNamesReconciled <- formulaVar #grab the most recent year variable names if available
    }
    
    formulaID <- codebook[codebook$Labels %in% descriptiveVarDesc & codebook$grade==grade , "ID"]
    varTypes <- codebook[codebook$Labels %in% descriptiveVarDesc & codebook$grade==grade , "categoricalOrContinuous"]
    
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
    hasData <- countries %in% esdfl$covs$country
    
    #====Loop Through Each Country 
    for(iCntry in seq_along(countries)){
      
      cntry <- countries[iCntry]
      
      if(hasData[iCntry]){ #has data entry
        
        if(DB_CACHE_ACTIVE && hasSummaryResult_Cached.TIMSS(formulaID = formulaID,
                                                             weightVar = weightVar,
                                                             grade = grade, 
                                                             year = yr,
                                                             country = cntry)){ #check if result is available
          
          resultList[[iResult]] <- getSummaryResult_Cached.TIMSS(formulaID = formulaID,
                                                                 weightVar = weightVar,
                                                                 grade = grade,
                                                                 year = yr,
                                                                 country = cntry)
        }else{ #calculate
          
          #determine the index position as they might be different
          lookupIdx <- which(esdfl$covs$country==cntry)
          esdf <- esdfl$datalist[[lookupIdx]] #get the edsurvey.data.frame object needed for the analytic call
          
          resultList[[iResult]] <- getSummaryResult_Calc.TIMSS(formulaID =  formulaID, 
                                                               formulaVar = formulaVar,
                                                               esdf =  esdf, 
                                                               weightVar = weightVar,
                                                               grade = grade,
                                                               year = yr,
                                                               country = cntry)
        }
      }else{ #no edsurvey.data.frame for year/country, just return an item with NA result item
        
        resultList[[iResult]] <- summaryResultItem.TIMSS(formulaID = formulaID,
                                                         formulaVar = formulaVar,
                                                         grade = grade,
                                                         year = yr,
                                                         country = cntry,
                                                         weight = weightVar,
                                                         result = NULL)
      }#end if(hasData[iCntry])
      
      iResult <- iResult + 1
    }#end for(iCntry in cntryIdx)
    
  }#end for(yr in years)
  
  #the call was unable to be calculated for any country/year for the requested variables
  if(length(varNamesReconciled)==0){
    stop(paste0("Unable to run the analysis based on your selections.  Check variable availabilty for year(s) selected and retry."))
  }
  
  #format our result list for returning back
  resultDF <- formatSummaryResultList.TIMSS(resultList = resultList, 
                                            formulaVar = varNamesReconciled, 
                                            varType = varTypes[1],
                                            weightVar = weightVar)
  

  return(resultDF)
}

#make the EdSurvey summary2 call and returns a 'summary.result.TIMSS' of the call.
getSummaryResult_Calc.TIMSS <- function(formulaID, formulaVar, esdf, weightVar, grade, year, country){

  res <- NULL
  
  processArgs <- list(data = esdf,
                      variable = formulaVar,
                      weightVar = weightVar)
  
  success <- tryCatch({
                        res <- do.call("summary2", args = processArgs, quote = FALSE)
                        
                        TRUE
                      }, error = function(e){
                        message(paste0("Error in summary2 Calc! Country:", country, "|Year:", year, "|Grade:", grade, "|Message: ", e$message))
                        FALSE
                      })
  
  if(success && !is.null(res)){
    #built the result item list for the summary.result that includes all of the information regarding this
    resItem <- summaryResultItem.TIMSS(formulaID = formulaID,
                                       formulaVar = formulaVar,
                                       grade = grade,
                                       year = year,
                                       country = country,
                                       weight = weightVar,
                                       result = res)
    
    #cache this object to a blob for faster retrieval later
    #only cache if the result is valid!
    cacheSummaryResult.TIMSS(formulaID = formulaID,
                             formulaVar = formulaVar,
                             weightVar = weightVar,
                             grade = grade,
                             year = year,
                             country = country,
                             resultObj = resItem)
    
  }else{
    
    #built the result item list for the summary.result that includes all of the information regarding this
    resItem <- summaryResultItem.TIMSS(formulaID = formulaID,
                                       formulaVar = formulaVar,
                                       grade = grade,
                                       year = year,
                                       country = country,
                                       weight = weightVar,
                                       result = NULL)
  }

  return(resItem)
}

#constructor for building a summary.result.TIMSS object to help keep things organized
summaryResultItem.TIMSS <- function(formulaID, formulaVar, grade, year, country, weight, result = NULL){

  resItem <- list(formulaID = formulaID,
                  formulaVar = formulaVar,
                  grade = grade,
                  year = as.character(year),
                  country = country,
                  weight = weight,
                  result = result) #build our list object meeting our object specification
  
  class(resItem) <- "summary.result.TIMSS"
  
  return(resItem)
}

#this returns the result from the cached items for the exact call per grade/year/country
getSummaryResult_Cached.TIMSS <- function(formulaID, weightVar, grade, year, country){

  resultSpec <- "SELECT resultObj FROM summary_result WHERE formulaID = ? AND 
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
                    TIMSS_summary_MagicNumber)
  
  #unable to specify a NULL value as an unnamed parameter here
  if(is.null(weightVar)){
    resultSpec <- sub("weight = ?", "weight is null", resultSpec, fixed = TRUE)
    paramList[[5]] <- NULL #drop the weightVar from the parameters list
  }
  
  res <- dbGetQuery(dbConnectPool, statement = resultSpec, params = paramList)
  res <- res[1,1] #this a list here since that's what we passed during INSERT
  
  #convert from json serialized object back to list object
  listObj <- jsonlite::unserializeJSON(res)
  listObj$year <- year #ensure it's same data type here, seems to do implicit conversion!
  
  return(listObj)
}

hasSummaryResult_Cached.TIMSS <- function(formulaID, weightVar, grade, year, country){

  countSpec <- "SELECT Count(*) FROM summary_result WHERE 
                    formulaID = ? AND
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
                    TIMSS_summary_MagicNumber)
  
  #unable to specify a NULL value as an unnamed parameter here with dbGetQuery call
  if(is.null(weightVar)){
    countSpec <- sub("weight = ?", "weight is null", countSpec, fixed = TRUE)
    paramList[[5]] <- NULL #drop the weightVar from the parameters list
  }
  
  res <- dbGetQuery(dbConnectPool, statement = countSpec, params = paramList)
  
  return(res[1,1]>0) #return TRUE/FALSE based on the count.  The res object will only have 1 row and 1 column containing the count value
}

cacheSummaryResult.TIMSS <- function(formulaID, formulaVar, weightVar, grade, year, country, resultObj){

  #if the cache is not active, then immediately return NULL here
  if(DB_CACHE_ACTIVE==FALSE){
    return(invisible(NULL))
  }
  
  #don't cache the record if the country didn't have the variables
  #this means that the resultObject was NULL and the formula vars are not available
  if(anyNA(formulaID) || anyNA(formulaVar)){
    return(invisible(NULL))
  }
  
  esv <- paste(utils::packageVersion("EdSurvey"), sep=".")
  json <- jsonlite::serializeJSON(resultObj)
  
  insertSpec <- "INSERT INTO summary_result (formulaID, 
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
  formulaVar <- paste0(formulaVar, collapse = "||")
  
  weightVar <- ifelse(is.null(weightVar), NA, weightVar) #NA is stored as NULL in this instance
  
  paramList <- list(formulaID,
                    formulaVar,
                    grade,
                    year,
                    country,
                    weightVar,
                    json,
                    esv,
                    TIMSS_summary_MagicNumber)
  
  res <- dbExecute(dbConnectPool, statement = insertSpec, params = paramList)
  
  if(res!=1){
    warning(paste0("Cache Insert Failed!"))
  }
  
  return(invisible(NULL))
}

#format a list of 'summary.result.TIMSS' objects to return a properly formatted data.frame result
formatSummaryResultList.TIMSS <- function(resultList, formulaVar, varType, weightVar){

  isWeighted <- !is.null(weightVar) && tolower(weightVar)!="null"
  
  #the summaryCols should match the exact names and order from an EdSurvey summary2 result
  if(grepl("^categorical", varType, ignore.case = TRUE)){
    if(isWeighted){ #weighted
      summaryCols <- c(formulaVar, "N", "Weighted N", "Weighted Percent", "Weighted Percent SE")
    }else{ #unweighted summary
      summaryCols <- c(formulaVar, "N", "Percent")
    }
    
  }else if(grepl("^continuous", varType, ignore.case = TRUE)){
    if(isWeighted){
      summaryCols <- c("Variable", "N", "Weighted N", "Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.", "SD", "NA's", "Zero-weights")
    }else{
      summaryCols <- c("Variable", "N", "Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.", "SD", "NA's")
    }
     
  }else{
    stop(paste0("Unknown ", sQuote("varType"), " specified in formatSummaryResultList.TIMSS: ", varType))
  }
  
  
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
      tempDF <- item$result$summary #this needs match the 'summaryCols' specification
    }else{ #NO DATA EXISTS OR HAD ERROR when running
      tempDF <- modelDF
      tempDF[1, ] <- NA # 
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
  cn2 <- cn[cn %in% c(formulaVar, "Variable")] #the formula variables or 'Variable' if categorical
  cn3 <- cn[!(cn %in% c(cn1, cn2, "Weight"))] #the rest of the columns as originally ordered
  cn4 <- cn[cn %in% c("Weight")] #this will be last
  outDF <- outDF[ , c(cn1, cn2, cn3, cn4)]
  
  return(outDF)
}