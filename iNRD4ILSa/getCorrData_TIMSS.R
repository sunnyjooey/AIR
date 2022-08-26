
#calculates the cross tabulated statistics for a an outcome variable and grouping variable(s) using the edsurveyTable EdSurvey function
#returns a formatted data.frame with all the variables necessary for the application

TIMSS_correlation_MagicNumber <- 2

getCorrData.TIMSS <- function(shinyILSAData, codebook, selectedVars, grade, years, countries, varADesc, varBDesc, method, weightVar = weightVar){
  
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
  iResult <- 1 #index for appending items to our result list
  
  for(yr in years){
    
    #the actual variables names 
    formulaVar_A <- codebook[codebook$Labels %in% varADesc & codebook$grade==grade , paste0("variableName", yr)]
    formulaVar_B <- codebook[codebook$Labels %in% varBDesc & codebook$grade==grade , paste0("variableName", yr)]
    isValidFormula <- !anyNA(formulaVar_A) && !anyNA(formulaVar_B) #checks if the formula is valid by having the requested variable names for that year
    
    if(isValidFormula){
      formulaID_A <- codebook[codebook$Labels %in% varADesc & codebook$grade==grade , "ID"]
      formulaID_B <- codebook[codebook$Labels %in% varBDesc & codebook$grade==grade , "ID"]
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
        
        if(DB_CACHE_ACTIVE && hasCorrResult_Cached.TIMSS(formulaID_A = formulaID_A,
                                                          formulaID_B = formulaID_B,
                                                          weightVar = weightVar,
                                                          grade = grade, 
                                                          year = yr,
                                                          country = cntry,
                                                          method = method)){ #check if result is available
          
          resultList[[iResult]] <- getCorrResult_Cached.TIMSS(formulaID_A = formulaID_A,
                                                              formulaID_B = formulaID_B,
                                                              weightVar = weightVar,
                                                              grade = grade,
                                                              year = yr,
                                                              country = cntry,
                                                              method = method)
        }else{ #calculate
          
          #determine the index position as they might be different
          lookupIdx <- which(esdfl$covs$country==cntry)
          esdf <- esdfl$datalist[[lookupIdx]] #get the edsurvey.data.frame object needed for the analytic call
          
          resultList[[iResult]] <- getCorrResult_Calc.TIMSS(formulaID_A =  formulaID_A,
                                                            formulaVar_A = formulaVar_A,
                                                            formulaID_B = formulaID_B,
                                                            formulaVar_B = formulaVar_B,
                                                            esdf =  esdf, 
                                                            weightVar = weightVar,
                                                            grade = grade,
                                                            year = yr,
                                                            country = cntry,
                                                            method = method)
        }
      }else{ #no edsurvey.data.frame for year/country OR invalid formula call for the year. just return an item with NULL result item
        resultList[[iResult]] <- corrResultItem.TIMSS(formulaID_A = NA,
                                                      formulaVar_A = NA,
                                                      formulaID_B = NA,
                                                      formulaVar_B = NA,
                                                      grade = grade,
                                                      year = yr,
                                                      country = cntry,
                                                      weight = weightVar,
                                                      method = method,
                                                      result = NULL)
      }#end if(hasCountry[iCntry] && isValidFormula)
      
      iResult <- iResult + 1
    }#end for(iCntry in cntryIdx)
    
  }#end for(yr in years)
  
  #format our result list for returning back
  resultDF <- formatCorrResultList.TIMSS(resultList = resultList, 
                                         formulaVar_A = formulaVar_A,
                                         formulaVar_B = formulaVar_B,
                                         method = method,
                                         weightVar = weightVar)
  return(resultDF)
}

#make the EdSurvey summary2 call and returns a 'summary.result.TIMSS' of the call.
getCorrResult_Calc.TIMSS <- function(formulaID_A, formulaVar_A, formulaID_B, formulaVar_B, esdf, weightVar, grade, year, country, method){
  
  res <- NULL

  #use this technique of list arguments and do.call to avoid headaches
  #due to parsing variables into literal strings across different environments!!
  processArgs <- list(x = formulaVar_A,
                      y = formulaVar_B,
                      data = esdf,
                      method = method,
                      weightVar = weightVar
                      )
  

  
  success <- tryCatch({
    res <- do.call("cor.sdf", args = processArgs, quote = FALSE) #EdSurvey::cor.sdf call

    TRUE
  }, error = function(e){
    message(paste0("Error in cor.sdf Calc! Country:", country, "|Year:", year, "|Grade:", grade, "|Message: ", e$message))
    FALSE
  })
  
  if(success && !is.null(res)){
    
    #built the result item list for the summary.result that includes all of the information regarding this
    resItem <- corrResultItem.TIMSS(formulaID_A = formulaID_A,
                                    formulaVar_A = formulaVar_A,
                                    formulaID_B = formulaID_B,
                                    formulaVar_B = formulaVar_B,
                                    grade = grade,
                                    year = year,
                                    country = country,
                                    weight = weightVar,
                                    method = method,
                                    result = res)
    
    #cache this object to a blob for faster retrieval later
    #only cache if result is valid!
    cacheCorrResult.TIMSS(formulaID_A = formulaID_A,
                          formulaVar_A = formulaVar_A,
                          formulaID_B = formulaID_B,
                          formulaVar_B = formulaVar_B,
                          weightVar = weightVar,
                          grade = grade,
                          year = year,
                          country = country,
                          method = method,
                          resultObj = resItem)
    
  }else{
    
    #built the result item list for the summary.result that includes all of the information regarding this
    resItem <- corrResultItem.TIMSS(formulaID_A = formulaID_A,
                                    formulaVar_A = formulaVar_A,
                                    formulaID_B = formulaID_B,
                                    formulaVar_B = formulaVar_B,
                                    grade = grade,
                                    year = year,
                                    country = country,
                                    weight = weightVar,
                                    method = method,
                                    result = NULL)
  }
  
  return(resItem)
}

#constructor for building a summary.result.TIMSS object to help keep things organized
corrResultItem.TIMSS <- function(formulaID_A, formulaVar_A, formulaID_B, formulaVar_B, grade, year, country, weight, method, result = NULL){
  
  resItem <- list(formulaID_A = formulaID_A,
                  formulaVar_A = formulaVar_A,
                  formulaID_B = formulaID_B,
                  formulaVar_B = formulaVar_B,
                  grade = grade,
                  year = as.character(year),
                  country = country,
                  weight = weight,
                  method = method,
                  result = result) #build our list object meeting our object specification
  
  class(resItem) <- "correlation.result.TIMSS"
  return(resItem)
}

#this returns the result from the cached items for the exact call per grade/year/country
getCorrResult_Cached.TIMSS <- function(formulaID_A, formulaID_B, weightVar, grade, year, country, method){
  
  resultSpec <- "SELECT resultObj FROM correlation_result WHERE formulaID_A = ? AND 
                                                                formulaID_B = ? AND 
                                                                grade = ? AND
                                                                year = ? AND 
                                                                country = ? AND 
                                                                weight = ? AND 
                                                                method = ? AND
                                                                magic_number = ? AND
                                                                force_recalc = 0"
  
  #param list must be in exact order of the SQL query since we are using unnamed parameters
  paramList <- list(formulaID_A,
                    formulaID_B,
                    grade,
                    year,
                    country,
                    weightVar,
                    method,
                    TIMSS_correlation_MagicNumber) #magic_number
  
  #unable to specify a NULL value as an unnamed parameter here
  if(is.null(weightVar)){
    resultSpec <- sub("weight = ?", "weight is null", resultSpec, fixed = TRUE)
    paramList[[6]] <- NULL #drop the weightVar from the parameters list
  }
  
  res <- dbGetQuery(dbConnectPool, statement = resultSpec, params = paramList)
  res <- res[1,1] #this a list here since that's what we passed during INSERT
  
  listObj <- jsonlite::unserializeJSON(res)
  listObj$year <- year #ensure it's same data type here, seems to do implicit conversion!
  
  return(listObj)
}

hasCorrResult_Cached.TIMSS <- function(formulaID_A, formulaID_B, weightVar, grade, year, country, method){
  
  countSpec <- "SELECT Count(*) FROM correlation_result WHERE formulaID_A = ? AND 
                                                              formulaID_B = ? AND 
                                                              grade = ? AND
                                                              year = ? AND 
                                                              country = ? AND 
                                                              weight = ? AND 
                                                              method = ? AND
                                                              magic_number = ? AND
                                                              force_recalc = 0"
  #param list must be in exact order of the SQL query since we are using unnamed parameters 
  paramList <- list(formulaID_A,
                    formulaID_B,
                    grade,
                    year,
                    country,
                    weightVar,
                    method,
                    TIMSS_correlation_MagicNumber)
  
  #unable to specify a NULL value as an unnamed parameter here with dbGetQuery call
  if(is.null(weightVar)){
    countSpec <- sub("weight = ?", "weight is null", countSpec, fixed = TRUE)
    paramList[[6]] <- NULL #drop the weightVar from the parameters list
  }
  
  res <- dbGetQuery(dbConnectPool, statement = countSpec, params = paramList)
  
  return(res[1,1]>0) #return TRUE/FALSE based on the count.  The res object will only have 1 row and 1 column containing the count value
}

cacheCorrResult.TIMSS <- function(formulaID_A, formulaVar_A,
                                  formulaID_B, formulaVar_B, 
                                  weightVar, grade, year, country, method, resultObj){
  
  #if the cache is not active, then immediately return NULL here
  if(DB_CACHE_ACTIVE==FALSE){
    return(invisible(NULL))
  }
  
  esv <- paste(utils::packageVersion("EdSurvey"), sep=".")
  json <- jsonlite::serializeJSON(resultObj) #serialize this to json string format for use later
  
  insertSpec <- "INSERT INTO correlation_result (formulaID_A, formulaVar_A,
                                                  formulaID_B, formulaVar_B,
                                                  grade, year, country, weight, method, resultObj,
                                                  edsurvey_version, magic_number) 
                                                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
  
  weightVar <- ifelse(is.null(weightVar), NA, weightVar) #NA is stored as NULL in this instance
  
  #for handling summaries with multiple variables collapse them with double pipe delimiter
  paramList <- list(formulaID_A,
                    formulaVar_A,
                    formulaID_B,
                    formulaVar_B,
                    grade,
                    year,
                    country,
                    weightVar, #weight
                    method,
                    json, #resultObj
                    esv, #edsurvey_version
                    TIMSS_correlation_MagicNumber) #magic_number
  
  res <- dbExecute(dbConnectPool, statement = insertSpec, params = paramList)
  
  if(res!=1){
    warning(paste0("Cache Insert Failed!"))
  }
  
  return(invisible(NULL))
}

#format a list of 'summary.result.TIMSS' objects to return a properly formatted data.frame result
formatCorrResultList.TIMSS <- function(resultList, formulaVar_A, formulaVar_B, method, weightVar){
  
  isWeighted <- !is.null(weightVar) && tolower(weightVar)!="null"
  
  summaryCols <- c("VarA", "VarB", "Full data N", "N Used", "Cor", "S.E.", "Method")
  
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
    
    tempDF <- modelDF
    tempDF[1, ] <- NA #all NA values to start

    tempDF$VarA <- item$formulaVar_A
    tempDF$VarB <- item$formulaVar_B
    tempDF$Method <- item$method
    
    #specific result values
    if(!is.null(item$result)){
      tempDF$`Full data N` <- item$result$n0
      tempDF$`N Used` <- item$result$nUsed
      tempDF$Cor <- item$result$correlation
      tempDF$`S.E.` <- item$result$Zse
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
  cn2 <- cn[cn %in% c(summaryCols)] #the formula variables or 'Variable' if categorical
  cn3 <- cn[!(cn %in% c(cn1, cn2, "Weight"))] #the rest of the columns as originally ordered, excluding grouping vars and weight
  cn4 <- cn[cn %in% c("Weight")] #this variable should be last
  outDF <- outDF[ , c(cn1, cn2, cn3, cn4)]
  
  return(outDF)
}