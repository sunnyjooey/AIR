source("dataController_TIMSS.R", local = TRUE) #ensure to source before 'sharedLoad.R'
source("dataCacheDB.R", local = TRUE)
source("getSummaryTabData_TIMSS.R", local = TRUE)
source("getDescriptiveData_TIMSS.R", local = TRUE)
source("getCorrData_TIMSS.R", local = TRUE)

source("sharedLoad.R",local = TRUE)
source("plotFunctions.R",local = TRUE)
source("helperFunctions.R",local = TRUE)

# sideBarBasicUI provides the basics elements for a sidebarPanel
sideBarBasicUI <- function(id) {
  # NS creates unique IDs so that inputs and outputs can be passed along to shiny server functions
  ns <- NS(id)
  uiOutput(ns("basicChooser"))
}

# empty, but needed to call on the server.R end
sideBarBasic <- function(id) {
  moduleServer(
    id,
    function(input, output, session, data = shinyILSAData, country_data = country_cats) {
  ########### Grade Input
  current_surveyChoice <- reactive({
    sampleCompareChoice <- c("Grade 4", "Grade 8")
    sampleCompareChoice
  })

  ### grade UI
  output$surveyChoice <- renderUI({
    ns <- session$ns
    selectizeInput(ns("sample"), label = "Grade",

                choices = current_surveyChoice(),
                options = list('plugins' = list('remove_button'),
                  title = 'Grade',
                  onInitialize = I('function() { this.setValue("");}')),
                selected = ifelse(length(current_surveyChoice()) == 1, current_surveyChoice(), ""))
    })


  ########### Year Input
  current_yearChoice <- reactive({
    if (invalidInput(input$sample)) {
      yearCompareChoice <- ""
    } else {
      yearCompareChoice <- list( `Select Options` = c("All Years","No Years"), Years = c("2011","2015") )
    }
    yearCompareChoice
    })

  ### year UI
  output$yearChoice <- renderUI({
    ns <- session$ns
    selectizeInput(ns("year"), label = "Year",
                choices = current_yearChoice(), multiple = TRUE,
                options = list('plugins' = list('remove_button'),
                title = 'Year',
                onInitialize = I('function() { this.setValue("");}')),
                selected = ifelse(length(current_yearChoice()) == 1, current_yearChoice(), ""))
    })

  ### observe changes - year
  observe({
    if(!is.null(input$year)){
      choices <- current_yearChoice()
      if(any(input$year =="All Years")){
        selected_choices <- current_yearChoice()$Years
        updatePickerInput(session, "year",
                          selected = selected_choices,
                          choices = choices)
      }
      if(any(input$year =="No Years")){
        updateSelectizeInput(session = getDefaultReactiveDomain(),"year",
                             choices = list( `Select Options` = c("All Years","No Years"), Years = c("2011","2015") ) ,
                             options = list('plugins' = list('remove_button')))
      }
    }
  })

  ########### Jurisdiction input 
  countries <- reactive({
    if (invalidInput(input$sample) | invalidInput(input$year)) {
      country_choices <- ""
    } else {
      year <- paste0("T",gsub(".*(\\d{2}$)","\\1" ,input$year))
      grade <- paste0("G",gsub(".*(\\d{1})","\\1" ,input$sample))
      sample <- paste(year, grade, sep = "_")

      if(length(sample) > 1){
        countries <- c()
        for(s in sample){
          countries <- c(countries,unlist(data[[s]]["covs"][[1]]))
        }
        countries <- data.frame(country = unique(countries))
      } else {
        countries <- data[[sample]]["covs"][[1]]
      }
      if(is.null(countries)) {
        countries <- data["covs"][[1]]
      }
      # merge in jurisdiction information
      countries <- base::merge(countries,
                               country_data,
                               by.x = "country",
                               by.y = "JurisdictionL2",
                               all.y = TRUE)
      # only those with data
      year <- as.numeric(input$year)
      grade <- as.numeric(gsub("[[:alpha:]]", "", input$sample))
      columns <- !colnames(countries) %in% "YearL2"
      countries <- unique(countries[countries$HasData == TRUE & countries$YearL2 %in% year & countries$GradeL1 %in% grade, columns])
      # get unique cases
      jurisdictions <- unique(countries$JurisdictionL1)
      # as far as I can tell this is an issue with the underlying data, for now I just make a gsub to fix the spelling
      # make list structure for displaying sub-groups in a selectizeInput()
      # see: https://stackoverflow.com/questions/50474865/pass-multiple-values-for-each-selected-option-in-selectinput-r-shiny/50494080
      country_choices <- list()
      names <- c()
      for(i in 1:length(jurisdictions)){
        # choices for each sub-group
        choices <- countries[countries$JurisdictionL1 == jurisdictions[i], "country"]
        country_choices <- c(country_choices, list(c(paste("All", jurisdictions[i]),paste("No", jurisdictions[i]))))
        # special case for displaying a singleton in group
        if(length(choices) == 1){
          choices <- list(choices)
          names(choices) <- choices
          country_choices <- c(country_choices, list(choices))
        } else {
          # else just add the choices in the subgroup to the list
          country_choices <- c(country_choices, list(choices))
        }
        names <- c(names, c(paste(jurisdictions[i], "Select Options"), jurisdictions[i]))
      }
      country_choices <- c(list(c("Select All","Select None")), country_choices)
      names <- c("Select Options", names)
      # name sub-group
      names(country_choices) <- names
    }
    # return
    country_choices
    })

  ### observe changes - jurisdiction
  observe({
    if(!is.null(input$statistic) ){
      choices <- countries()
      if (any(grepl("^All", input$statistic))) {
        allgroups <- input$statistic[input$statistic %in% grep("All|No", input$statistic, value = T )]
        alls <- allgroups[allgroups == grep("All", allgroups, value = T )]
        groups <- gsub("All\\s|No\\s","", allgroups)
        selected_choices <- c()
        for(group in groups){
          result <- try(unlist(countries()[[group]]))
          if(class(result) != "try-error"){
            selected_choices <- c(selected_choices,unlist(countries()[[group]]))
          } else {
            selected_choices <- c(selected_choices,unlist(countries()[group]))
          }
        }
        selected_choices <- c(input$statistic[!input$statistic %in% alls], selected_choices)
        updateSelectizeInput(session, "statistic",
                             selected = selected_choices, 
                             choices = choices) 
        } 

      else if (any(grepl("^No", input$statistic))) {
        allgroups <- input$statistic[input$statistic %in% grep("No", input$statistic, value = T )]
        alls <- allgroups[allgroups == grep("No", allgroups, value = T )]
        groups <- gsub("No\\s","", allgroups)
        selected_choices <- c()
        for(group in groups){
          result <- try(unlist(countries()[[group]]))
          if(class(result) != "try-error"){
            selected_choices <- c(selected_choices,unlist(countries()[[group]]))
          } else {
            selected_choices <- c(selected_choices,unlist(countries()[group]))
          }
        }
        selected_choices <- c(input$statistic[!input$statistic %in% c(alls,selected_choices)])
        print(selected_choices)
        updateSelectizeInput(session, "statistic",
                          selected = selected_choices,
                          choices = choices)
      }
      else if(any(input$statistic == "Select All")){
        selected_choices <- unname(unlist(choices))
        selected_choices <- selected_choices[-grep("^No|^All|Select",selected_choices)]
        updateSelectizeInput(session, "statistic",
                          selected = selected_choices,
                          choices = choices)

      }
      else if(any(input$statistic == "Select None")){
        updateSelectizeInput(session = getDefaultReactiveDomain(), "statistic",
                          selected = NULL,
                          choices = choices,
                          options = list('plugins' = list('remove_button')))
      }
      # skip 'else' - it will break the app
    }
  })
  
  ### jurisdiction UI
  output$statisticsChoice <- renderUI({
    ns <- session$ns
    selectizeInput(ns("statistic"), label = "Country",
                choices = countries(), multiple = TRUE,
                options = list('plugins' = list('remove_button'),
                title = 'Country',
                onInitialize = I('function() { this.setValue("");}')),
                selected = ifelse(length(countries()) == 1, countries(), ""))
    })


  ########### Reset Criteria button
  output$resetCriteria <- renderUI({
    # ns
    ns <- session$ns
    # we only want to see the reset options after all selections
    shiny::validate({
      need(input$sample, message = FALSE)
      need(input$year, message = FALSE)
      need(input$statistic, message = FALSE)
    })
    
    actionButton(ns("reset"), "Reset Criteria")
  })

  ### observe - reset button click
  observeEvent(input$reset, {
    # ns <- session$ns
    session$sendCustomMessage("reSet", "reset");
    shinyjs::reset("sample")
    shinyjs::reset("year")
    shinyjs::reset("survey")
    data <- list(tab = "select")
    session$sendCustomMessage(type='updateSelections', data)
    disable(selector = '#analyzePanel li a[data-value=descriptive2]')
    disable(selector = '#analyzePanel li a[data-value=summaryTab]')
    disable(selector = '#analyzePanel li a[data-value=corrTab]')
    disable(selector = '#mainPanel li a[data-value=analyze]')
    print(input$sample)
    print(input$year)
    print(input$survey)
  })

  ### sidebarUI
  output$basicChooser <- renderUI({
    ns <- session$ns
    tagList(
      tags$div(
               fluidRow(
                 column(12,
                        # grade
                        uiOutput(ns("surveyChoice"))),
                 column(12,
                        # year
                        uiOutput(ns("yearChoice"))),
                 column(12,
                        # country
                        tags$style(type='text/css', ".selectize-dropdown-content {max-height: 800px; }"), 
                        uiOutput(ns("statisticsChoice"))),
                 column(12, 
                        # variables
                        uiOutput(ns("variable_ui"))),
                 column(12,align="right",
                        # add line breaks
                        linebreaks(3),
                        # reset button
                        uiOutput(ns("resetCriteria")))
      )))
  })
}
  )
} # end 'sideBarBasic' function



# sideBarControlPanel provides the elements for plot customization
sideBarControlPanelUI <- function(id) {
  ns <- NS(id)
  tagList(
    uiOutput(ns("plotCustomDownloadToggle")),
    conditionalPanel(condition = paste0("input.downloadPlot % 2 == 1"), ns = ns,
                     uiOutput(ns("plotCustomDownload"))
    )
  )
}


sideBarControlPanel <- function(id) {
  moduleServer(
    id,
      function(input, output, session, data) {

    # Download Plot tab
    output$plotCustomDownloadToggle <- renderUI({
      ns <- session$ns
      tagList(
        actionButton(ns("downloadPlot"), "Download Plot",style='font-size:100%; color:#FFFFFF; background-color:#0E334F', width = '100%')
      )
    })

    output$plotCustomDownload <- renderUI({
      ns <- session$ns
      plotHeight <- 10
      plotWidth <- 15
      tagList(
        textInput(ns('plotExportHeight'), 'Height', value = plotHeight),
        textInput(ns('plotExportWidth'), 'Width', value = plotWidth),
        selectInput(ns('plotExportType'), 'Type', choices = c('png','pdf','svg'), selected  = 'png'),
        downloadButton(ns("plotCustomDownloadEvent"), "Download Plot", style = "background:lightgreen; width:100%;")
      )
    })
  }
  )}

# plotTableUI provides the elements for plotting tables
plotTableUI <- function(id) {
  ns <- NS(id)
  tagList(
    tabsetPanel(id = "mainPanel",
     tabPanel(HTML("<h2>Step 2: Select Variables</h2>"),
                           br(),
                           br(),
                           withSpinner(uiOutput(ns("variableSelectionAccordion"))),
                           value = "select"
                  ),
     tabPanel(HTML("<h2>Step 3: Conduct Analysis</h2>"), 
       tabsetPanel(id = "analyzePanel",
                   tabPanel("Descriptive statistics",  uiOutput(ns("descriptive2tab")), value = "descriptive2"),
                   tabPanel("Summary table",  uiOutput(ns("summaryTab")), value = "summaryTab"),
                   tabPanel("Correlation analysis",  uiOutput(ns("corrTab")), value = "corrTab"),
                   selected="descriptive2"), 
       value = "analyze")

                 
      
    )

  )
}



# plotTable accepts data as an argument, which is passes along to the reactive function, allowing the plotTableUI to create unique plots
# based on a subset of the data AND allows it to be reused with other data sets
plotTable <- function(id) {
  moduleServer(
    id,
      function(input, output, session, data, dimension) {

  ### Yuqi: I'm writing a new subsetCodebook reactive function "subsetCodebookNew" below to illustrate the idea that only one combined variable spreadsheet is needed as explained in https://github.com/American-Institutes-for-Research/iNRD4ILSa/issues/27#issuecomment-663901706. I'm keeping subsetCodebook so other analytical tabs won't be affected for now.
  subsetCodebookNew <- reactive({
    shiny::validate({
      need(input$sample, message = F)
      need(!is.null(input$sample), message = F)
      need(input$year, message = F)
      need(!is.null(input$year), message = F)
      need(input$statistic, message = F)
      need(!is.null(input$statistic), message = F)
    })

    all_files <- list.files("data/")
    the_file <- grep("T11_15_G4_G8_codebook", all_files, value = T)
    combinedCodebookFile <- read.csv(paste0("data/",the_file))

    # keep rows based on the selected grade (input$sample should be either "Grade 4" or "Grade 8")
    codebook <- combinedCodebookFile %>%
        filter(grade %in% input$sample)

    # keep rows based on the selected year (input$year could be "2011", "2015", or c("2011", "2015"))
    if(length(input$year) == 1){
      if (input$year == "2011"){
        codebook <- codebook %>%
          filter(!is.na(variableName2011))
      } else {
        codebook <- codebook %>%
          filter(!is.na(variableName2015))
      }
    } #else don't need to do any filtering
    return(codebook)
  })


  ### reactive function that responds to input in the sideBarBasicUI; this turns subsetCodebook into a function, which is reused to create output objects
  subsetSelectedSurvey <- reactive({
    shiny::validate({
      need(input$sample, message = F)
      need(!is.null(input$sample), message = F)
      need(input$year, message = F)
      need(!is.null(input$year), message = F)
      need(input$statistic, message = F)
      need(!is.null(input$statistic), message = F)
    })
    year <<- paste0("T",gsub(".*(\\d{2}$)","\\1" ,input$year))
    grade <<- paste0("G",gsub(".*(\\d{1})","\\1" ,input$sample))
    yearGrade <- paste0(year,"_", grade)
    yearGradeSelection <- which(names(shinyILSAData) %in% yearGrade)
    shinyILSADataSelection <- shinyILSAData[c(yearGradeSelection)]
    selectedCountries <- input$statistic
    shinyILSADataSelectionNames <- names(shinyILSADataSelection)
    if(length(shinyILSADataSelectionNames) > 1) {
      newDataList <- list()
      # grab data for each grade / year combo
      for(i in shinyILSADataSelectionNames) {
        selectedCountriesIndex <- which(shinyILSADataSelection[[i]]$covs$country %in% selectedCountries)
        selectedCountriesName <- selectedCountries[shinyILSADataSelection[[i]]$covs$country %in% selectedCountries]
        shinyILSADataSelectionIth <- shinyILSADataSelection[[i]]
        shinyILSADataSelectionIth <- shinyILSADataSelectionIth$datalist[selectedCountriesIndex]
        newDataList[[i]] <- shinyILSADataSelectionIth
      }
    } else {
    # grab data for single grade / year combo
    selectedCountriesIndex <- which(shinyILSADataSelection[[1]]$covs$country %in% selectedCountries)
    newDataList <- shinyILSADataSelection[[1]]$datalist[selectedCountriesIndex]
    }
    return(newDataList)
  })

  ### labels for codebook
  retrieveFilteredCodebookLabels <- reactive({
    # null case: return empty 
    if (is.null(input$selVars)) {
      return(c())
    } else {
      # non empty case: 
      codebook <- subsetCodebookNew()
      varOptions <- input$selVars
      codebookFiltered <- codebook %>% filter(ID %in% varOptions)
      codebookFiltered2 <<- codebook
      # break out continuous and categorical into categories
      cat <- codebookFiltered[codebookFiltered$categoricalOrContinuous == "Categorical variable", "Labels"]
      cont <- codebookFiltered[codebookFiltered$categoricalOrContinuous == "Continuous variable", "Labels"]
      # singleton case: we need to add an extra list() around vector 

      if(length(cat) == 1 ){
        cat <- list(cat)
      }
      
      if(length(cont) == 1){
        cont <- list(cont)
      }
      
      # add labels 
      # logic so we only display categories present 
      if(length(cont) == 0){
        # only Categorical 
        labelVector <- list("Categorical Variables" = cat)
      } else {
        if(length(cat) == 0){
          # only Continuous 
          labelVector <- list("Continuous Variables" =  cont)
        }  else {
          # both present 
          labelVector <- list("Categorical Variables" = cat, "Continuous Variables" =  cont)
        }
      }

      
      # return labels 
      return(labelVector)
    }
  })



  ### labels for codebook
  retrieveWeights <- reactive({
    # null case: return empty 
    if (is.null(input$selVars)) {
      return(c())
    } else {
      # create one variable for year and grade selections
      year <- paste0("T",gsub(".*(\\d{2}$)","\\1" ,input$year))
      grade <- paste0("G",gsub(".*(\\d{1})","\\1" ,input$sample))
      year_grade <<- paste(year, grade, sep = "_")
      # initialize a dataframe with two columns for the weight and weight label
      weight_labels_df <- data.frame(matrix(vector(), 0, 2, dimnames=list(c(), c("variableName", "Labels"))),
                                     stringsAsFactors=F)
      # add weights to the dataframe based on the year and grade selections
      for (i in year_grade){
        weight_options <- names(shinyILSAData[[i]]$datalist[[1]]$weights)
        ith_weight_labels_df <- searchSDF(string = "weight", data = shinyILSAData[[i]])
        ith_weight_labels_df <- ith_weight_labels_df[ith_weight_labels_df$variableName %in% weight_options,]
        ith_weight_labels_df <- ith_weight_labels_df[,c("variableName", "Labels")]
        weight_labels_df <- rbind(weight_labels_df, ith_weight_labels_df)
      }
      # remove duplicate rows
      weight_labels_df <- weight_labels_df[!duplicated(weight_labels_df[,'variableName',]),]
      # create a named vector to use in the pickerInput
      weight_vars <- c(weight_labels_df[,1], "NULL")
      names(weight_vars) <- c(weight_labels_df[,2], "NO WEIGHT")

      # retrieve default weight
      defaultWeight <- attributes(shinyILSAData[[year_grade[[1]]]][[1]][[1]]$weights)$default

      # return labels and default weight as a list to send to analytical tab; this method is used so that year_grade doesn't need to be referenced in the tabs
      weight_vars_list = list(weightValues = weight_vars, weightValuesDefault = defaultWeight)
      return(weight_vars_list)
    }
  })
  

  ########################################################################################################################
  #### ANALYTICAL TABS 
  ########################################################################################################################
  # start with anlytic tabs hidden 
  disable(selector = '#analyzePanel li a[data-value=descriptive2]')
  disable(selector = '#analyzePanel li a[data-value=summaryTab]')
  disable(selector = '#analyzePanel li a[data-value=corrTab]')
  disable(selector = '#mainPanel li a[data-value=analyze]')
  # reveal after selection 
  observeEvent(input$selVars, {
    enable(selector = '#analyzePanel li a[data-value=descriptive2]')
    enable(selector = '#analyzePanel li a[data-value=summaryTab]')
    enable(selector = '#analyzePanel li a[data-value=corrTab]')
    enable(selector = '#mainPanel li a[data-value=analyze]')
  })

  ### Descriptive2 Tab
  ### Descriptive2 Tab Render 
  output$descriptive2tab <- renderUI({
    ns <- session$ns
    labelValues <- retrieveFilteredCodebookLabels()
    names(labelValues) <- ifelse(names(labelValues) == "Categorical Variables", "Categorical Variables (Percent)", "Continuous Variables (Mean)") 
    weightValues <- retrieveWeights()
    
    # create selections for user
    tagList(
      # select variables 
      HTML("<h3>Instructions: </h3><h4>Get descriptive statistics for a variable by selecting a variable and weight.</h4><br>"),
      pickerInput(ns("variablesSelectedDescriptive2"), label = "Select Variable",
                  choices = labelValues, multiple = TRUE,
                  options = pickerOptions(title = 'Select Variable',
                                 maxOptions = 1,
                                 maxOptionsText = "Only one variable may be selected at a time.")),
      # weight option, not sure if this feature is fully implemented
      pickerInput(ns("weightDescriptive2"), label = "Select Weight",
                  choices = weightValues$weightValues, multiple = FALSE,
                  options = list(title = 'Select Weight'),
                  selected = weightValues$weightValuesDefault),  # preselect the default weight
      # display details, not sure if this feature is fully implemented 
      checkboxInput(ns("checkboxInputDetails"), label = "Display Details"),
      # run analytics 
      actionButton(ns("obsEventButtonDescriptive2tab"), "Run"),
      # download button 
      downloadButton(ns("downloadDescriptive2"), "Download Table"), 
      # table caption with white space padding 
      HTML("<br><br>"),
      uiOutput(ns("captionDescriptive2")), 
      HTML("<br><br>"),
      # table dispaly 
      withSpinner(tableOutput(ns("descriptive2TableOutput"))), 
      HTML("<br>"),
      uiOutput(ns("footnoteDescriptive2")), 
    )
  })
  

  
  # Descriptive 2 Outcome variable choice, Does this need to be limited to only allow certain variables?
  varOptionsOutcome3 <- eventReactive(input$obsEventButtonDescriptive2tab, {
    input$variablesSelectedDescriptive2
  })
  
  observeEvent(input$variablesSelectedDescriptive2,{
    jcode <- paste0('$(\'[data-id="mainTool-variablesSelectedDescriptive2"]\').prop("title","");')
    runjs(jcode)
  })
  
  observeEvent(input$variablesSelectedDescriptive2,{
    descriptiveVars <- paste(input$variablesSelectedDescriptive2, collapse = "\\n\\n")
    jcode <- paste0('$(\'[data-id="mainTool-variablesSelectedDescriptive2"]\').prop("title","',descriptiveVars,'");')
    runjs(jcode)
  })
  
  ### Descriptive2 Grouping Variable Selection
  varOptionsWeightDescriptive2 <- eventReactive(input$obsEventButtonDescriptive2tab, {
    input$weightDescriptive2
  })

  ### Descriptive2 Table Data Creation   
  # when the descriptive2 run button is clicked we build data
  descriptive2_df <- eventReactive(input$obsEventButtonDescriptive2tab, {
    # load dynamic data 
    codebook <- subsetCodebookNew()
    selectedData <- subsetSelectedSurvey()
    varOptions <- input$selVars
    selectedVars <- varOptionsOutcome3()
    selectedWeightDescriptive2 <- varOptionsWeightDescriptive2()
    # make sure only categorical or continuous is selected 
    if(length(unique(codebook[codebook$Labels %in% selectedVars, "categoricalOrContinuous"])) != 1){
      return(NULL)
      print('here in descriptive2_df null')
    }
    # getData call
    newDataOutput <- getDescriptiveData.TIMSS(shinyILSAData = shinyILSAData,
                                     codebook = codebook,
                                     selectedVars = input$selVars,
                                     grade = input$sample,
                                     years = sort(input$year),
                                     countries = input$statistic,
                                     descriptiveVarDesc = selectedVars,
                                     weightVar = selectedWeightDescriptive2)
    #return
    newDataOutput
  })
  

  
  ### Summary2 caption
  descriptive2_nt_info <- reactive({
      table <- descriptive2_df()
      # building blocks for foot note text 
      text <- "<b>SOURCE:</b> International Association for the Evaluation of Educational Achievement, Trends in International Mathematics and Science Study (TIMSS): "
      # year
      year <- paste0(paste0(unique(table[["Year"]]), collapse = ", "), " Mathmatics and Science Assessment")
      # build note, dynamic depending on missingness
      if("-" %in% table[["N"]] | "-" %in% table[["Weighted N"]])  {
        note <- "<b>NOTE:</b> Some apparent differences between estimates may not be statistically signficant."
        note <- c(note,"In case of <b>'-'</b> standards were either not met or data are not available for selected inputs.<br/>")
      } else {
        note <- "<b>NOTE:</b> Some apparent differences between estimates may not be statistically signficant.<br/>"
      }
      return(c(paste0(text, year, "."), year, note))
  })

  
  ### Descriptive2 caption
  output$footnoteDescriptive2 <- renderUI({
    # first generate descriptive2 table 
    info <- descriptive2_nt_info()
    text <- info[1]
    year <- info[2]
    note <- info[3]
    # define a fluid-row/column structure that includes text and image 
    # return 
    return(
        # fluid row start 
        fluidRow(
          # column text 
          column(12, align= "left", 
                 HTML(note), 
                 HTML(text)
          ) ,# column text end 
          # column image 
          column(12, align="right", style = "padding-top: 25px;",
                 div(#style = "align: right;",
                   tags$a(href='https://nces.ed.gov/timss/',
                          img(src="TIMSS-USA-logo.png")))
          ) # column image end 
        ) # fluid row end 
      ) # end of return 
  })


  ### Descriptive2 Tab Table Render - renders the dataframe this uses the output from the summaryTableDetailsReactive selection
  output$descriptive2TableOutput <- renderTable({
  	    shiny::validate({
      need(!is.null(descriptive2_df()), "Please select either all continuous, or all categorical variables for analysis.")
      need(!is.null(input$variablesSelectedDescriptive2), "Make selection(s) from all available dropdowns.")
    })
    ct2 <<- descriptive2_df()
    ct <- descriptive2_df()
    print(paste0("Weighted: ",input$weightDescriptive2))
    print(paste0("Checkbox: ",input$checkboxInputDetails))

    if (!is.null(input$weightDescriptive2)) { # use weights
      if (!input$checkboxInputDetails) { # don't display details (the default)
        cols_to_exclude <- c("N", "Weighted N", "1st Qu.", "Median", "3rd Qu.", "NA's", "Zero-weights")
        ct <- ct[,!colnames(ct) %in% cols_to_exclude]
      }
    } else { # exclude weights box checked
      if (!input$checkboxInputDetails) { # don't display details (the default)
        cols_to_exclude <- c("N", "1st Qu.", "Median", "3rd Qu.", "NA's")
        ct <- ct[,!colnames(ct) %in% cols_to_exclude]
      }
    }
    ct
  })

  
  ### Descriptive 2 Tab Download Handler 
  output$downloadDescriptive2 <- downloadHandler(
    filename = function() {
      paste0("Descriptive2", Sys.Date(), ".csv")
    },
    content = function(file) {
      out <- descriptive2_df()
      note <- descriptive2_nt_info()[c(1,3)]
      note <- gsub('<b>|</b>|<br/>', '',note)
      out[nrow(out) + 1, 1] <- note[2]
      out[nrow(out) + 1, 1] <- note[1]
      write_csv(out, file)
    }
  )

  ## Logo for Descriptive2, possible to use this UI element for other tabs if needed 
  # output$logo <- renderUI({
  #   # wait for table to finish 
  #   shiny::validate({
  #     need(descriptive2_df, "")
  #   })
  #   # define a right column, add padding, tag and image 
  #   column(12, align="right", style = "padding-top: 25px;",
  #          div(#style = "align: right;",
  #              tags$a(href='https://nces.ed.gov/timss/',
  #                     img(src="TIMSS-USA-logo.png")))
  #   )
  # })
  
  ########################################################################################################################
  ### summary Table Tab 
  ########################################################################################################################
  ### summary Table Render 
  output$summaryTab <- renderUI({
    ns <- session$ns
    labelValues <- retrieveFilteredCodebookLabels()
    names(labelValues) <- ifelse(names(labelValues) == "Categorical Variables", "Categorical Variables (Mean)", "Continuous Variables (Mean)") 
    weightValues <- retrieveWeights()

    tagList(
      HTML("<h3>Instructions: </h3><h4>Create a descriptive table of the outcome variable by selecting one or more grouping variables and a weight.</h4><br>"),
      pickerInput(ns("variablesSelectedOutcome"), label = "Select Outcome Variable",
                  choices = labelValues$`Continuous Variables`, multiple = FALSE,
                  options = list(title = 'Select Outcome Variable')),

      pickerInput(ns("unsortedGrouping"), label = "Select Grouping Variable(s)",
                  choices = c("No grouping (use full population)", labelValues$`Categorical Variables`), multiple = TRUE,
                  options = list(title = 'Select Grouping Variable(s)')),
      uiOutput(ns("variablesSelectedGrouping")),
      # weight option
      pickerInput(ns("weightSummaryTab"), label = "Select Weight",
                  choices = weightValues$weightValues, multiple = FALSE,
                  options = list(title = 'Select Weight'),
                  selected = weightValues$weightValuesDefault),  # preselect the default weight
      actionButton(ns("obsEventButton"), "Run"),
      downloadButton(ns("downloadSummaryTabData"), "Download Table"),
      HTML("<hr>"),
      checkboxInput(ns("checkboxInputDetails2"), label = "Display Details"),
      # table caption with white space padding 
      HTML("<br><br>"),
      uiOutput(ns("captionSummaryTab")), 
      # Table Display 
      HTML("<br><br>"),
      withSpinner(tableOutput(ns("summaryTableOutput"))), 
      # Foot Note Display 
      HTML("<br>"),
      uiOutput(ns("footnoteSummaryTab"))
    )
  })
  
  ### Summary Table Outcome Variable Selection
  varOptionsOutcome2 <- eventReactive(input$obsEventButton, {
    input$variablesSelectedOutcome
  })
  
  ### Summary Table Grouping Variable Selection
  
  rankGroups <- eventReactive(input$unsortedGrouping, {
    if(length(input$unsortedGrouping) < 2){
      return(NULL)
    } else {
      rank_list(
        text = "Drag the grouping variables to the desired order for grouping",
        labels = input$unsortedGrouping,
        input_id = session$ns("ranking") 
      )
    }
  })
  
  output$variablesSelectedGrouping <- renderUI({
    rankGroups()
  })
  
  varOptionsGrouping <- eventReactive(input$obsEventButton, {
    if(length(input$unsortedGrouping) < 2){
      if(input$unsortedGrouping == "No grouping (use full population)") {
        return(1)
      } else {
        input$unsortedGrouping
      }
    } else {
      input$ranking
    }
  })
  
  ### Summary Table Table Data Creation   
  # when the summary table run button is clicked we build data
  summarytable_df <- eventReactive(c(input$obsEventButton), {
    # dynamic data 
    codebook <- subsetCodebookNew()
    selectedData <- subsetSelectedSurvey()
    varOptions <- input$selVars
    selectedVarLHS <- varOptionsOutcome2()
    selectedVarRHS <- varOptionsGrouping()

    newDataOutput2 <- getSummaryTabData.TIMSS(shinyILSAData = shinyILSAData,
                                             codebook=codebook,
                                             selectedVars = varOptions,
                                             countries = input$statistic,
                                             grade = input$sample,
                                             years = sort(input$year),
                                             outcomeVarDesc = selectedVarLHS,
                                             groupingVarsDesc = selectedVarRHS,
                                             weightVar = input$weightSummaryTab) #default to totwgt for now until weight selection available

    #return
    newDataOutput2
  })
  
  ### Summary Tab caption
  # UI display 
  output$captionSummaryTab <- renderUI({
    # grab caption data for SummaryTab
    captionSummarydat() 
  })
  # data for caption 
  # observe event so measure names don't update before a new table is generated
  captionSummarydat <- eventReactive(input$obsEventButton, {
    # check for summarytab results 
    shiny::validate({
      need(!is.null(summarytable_df()), "")
    })
    # pull information from generated descriptive2 
    table <- summarytable_df()
    # grade 
    grade <- unique(table[["Grade"]])
    # year
    year <- paste0(unique(table[["Year"]]), collapse = ", ")
    # get grouping vars, also bold using lapply 
    grpVars <- paste0(unlist(lapply(input$variablesSelectedGrouping, function(x) { paste0("<b>", x, "</b>") })), collapse = ", ")
    # get outcome var 
    outcomeVar <- paste0("<b>", input$variablesSelectedOutcome, "</b>")
    # paste caption and return 
    return(HTML(paste0("Averages for ", grade, " TIMSS ", outcomeVar , " by ", grpVars, ": ", year)))
  }
  )
  
    ### crosstab caption
  cross_nt_info <- reactive({
    table <- summarytable_df()
    # building blocks for foot note text 
    text <- "<b>SOURCE:</b> International Association for the Evaluation of Educational Achievement, Trends in International Mathematics and Science Study (TIMSS): "
    # year
    year <- paste0(paste0(unique(table[["Year"]]), collapse = ", "), " Mathmatics and Science Assessment")
    # build note, dynamic depending on missingness
    if("-" %in% table[["N"]] | "-" %in% table[["Weighted N"]])  {
      note <- "<b>NOTE:</b> Some apparent differences between estimates may not be statistically signficant."
      note <- c(note,"In case of <b>'-'</b> standards were either not met or data are not available for selected inputs.<br/>")
    } else {
      note <- "<b>NOTE:</b> Some apparent differences between estimates may not be statistically signficant.<br/>"
    }
    return(c(paste0(text, year, "."), year, note))
  })

  ### Summary Tab Footnote
  output$footnoteSummaryTab <- renderUI({
    # check for summarytab results 
    shiny::validate({
      need(!is.null(summarytable_df()), "")
    })
    info <- cross_nt_info()
    text <- info[1]
    year <- info[2]
    note <- info[3]
    # define a fluid-row/column structure that includes text and image
    # return
    return(
      # fluid row start
      fluidRow(
        # column text
        column(12, align= "left",
               HTML(note),
               HTML(paste0(text, year, "."))
        ) ,# column text end
        # column image
        column(12, align="right", style = "padding-top: 25px;",
               div(#style = "align: right;",
                 tags$a(href='https://nces.ed.gov/timss/',
                        img(src="TIMSS-USA-logo.png")))
        ) # column image end
      ) # fluid row end
    ) # end of return
  })
  
  # function subsets the columns from the summaryTab dataframe object so the rendered results table can be reshaped like a flip switch
  summaryTableDetailsReactive <- eventReactive(input$obsEventButton, {
    shiny::validate({
      need(!is.null(summarytable_df()), "")
      need(!is.null(input$unsortedGrouping) & !is.null(input$variablesSelectedOutcome) , "Make selection(s) from all available dropdowns.")
    })
    ct <- summarytable_df()
    if (input$checkboxInputDetails2) {
      ct
    } else {
      ct <- ct[,!colnames(ct) %in% c("N", "WTD_N")]
    }
    ct
  })
  
  ### Summary Tab Render Table - renders the dataframe this uses the output from the summaryTableDetailsReactive selection
  output$summaryTableOutput <- renderTable({
    summaryTableDetailsReactive()
  })
  
  # Summary Tab download handler
  output$downloadSummaryTabData <- downloadHandler(
    filename = function() {
      paste0("SummaryTable", Sys.Date(), ".csv")
    },
    content = function(file) {
      out <- summarytable_df()
      note <- cross_nt_info()[c(1,3)]
      note <- gsub('<br>|<b>|</b>|<br/>', '',note)
      out[nrow(out) + 1, 1] <- note[2]
      out[nrow(out) + 1, 1] <- note[1]
      write_csv(out, file)
    }
  )
  ########################################################################################################################
  ### Corr Table Tab 
  ########################################################################################################################
  
  
# ======================================================= Corr Table UI elements ====================================================
  names <- c("Pearson","Spearman", "Polychoric", "Polyserial")

  output$corrTab <- renderUI({
    ns <- session$ns
    labelValues <- retrieveFilteredCodebookLabels()
    names(labelValues) <- ifelse(names(labelValues) == "Categorical Variables", "Categorical Variables (Percent)", "Continuous Variables (Percent)") 
    weightValues <- retrieveWeights()

    tagList(
      HTML("<h3>Instructions: </h3><h4>Calculate correlations by selecting two variables and a weight.</h4><br>"),
      pickerInput(ns("variablesSelectedCorrA"), label = "Variable A",
                  choices = labelValues, multiple = FALSE,
                  options = list(title = 'Select Variable A')),
      pickerInput(ns("variablesSelectedCorrB"), label = "Variable B",
                  choices = labelValues, multiple = FALSE,
                  options = list(title = 'Select Variable B')),
      # weight option
      pickerInput(ns("weightCorrTab"), label = "Weight",
                  choices = weightValues$weightValues, multiple = FALSE,
                  options = list(title = 'Select Weight'),
                  selected = weightValues$weightValuesDefault),  # preselect the default weight
      pickerInput(ns("correlationType"), label = "Correlation Type",
                  choices = names, multiple = FALSE,
                  selected = "Pearson",
                  options = list(title = 'Correlation Type')),
      uiOutput(ns("logicText")),
      # display details
      checkboxInput(ns("checkboxInputDetailsCorr"), label = "Display Details"),
      # run analytics
      actionButton(ns("obsEventButtonCorr"), "Run"),
      # download button
      downloadButton(ns("downloadCorrTabData"), "Export Results"),
      HTML("<hr>"),
      # table caption with white space padding 
      HTML("<br><br>"),
      uiOutput(ns("captionCor")), 
      # Table Display 
      HTML("<br><br>"),
      withSpinner(tableOutput(ns("corrTableOutput"))), 
      # Foot Note Display 
      HTML("<br>"),
      uiOutput(ns("footnoteCor"))
    )
  })
  
  # ==================================================  Corr logic text  ==================================================
 
  corrLogic <- eventReactive(c(input$variablesSelectedCorrA, input$variablesSelectedCorrB), {
    shiny::validate({
      need(input$variablesSelectedCorrA, "")
      need(input$variablesSelectedCorrB, "")
    })
   
    codebook <- subsetCodebookNew()

    vars <- paste0("<b>",input$variablesSelectedCorrA,"</b>", " and <b>", input$variablesSelectedCorrB, "</b>")
    # paste text and return 
    
    uniqueVarTypes <- unique(as.vector(codebook[codebook$Labels %in% c(input$variablesSelectedCorrA, input$variablesSelectedCorrB), "categoricalOrContinuous"]))
    if (length(uniqueVarTypes) == 1) {
      if (uniqueVarTypes == "Continuous variable") { # if all variables are continuous
        explanation <- HTML("<span>&#8505;</span> Pearson is the default correlation for ", vars, " because they are both continuous variables.")
        } else { # if all variables are categorical
        explanation <- HTML("<span>&#8505;</span> Point-polychoric is the default correlation for ", vars, " because they are both categorical variables.")
        }
      } else {
        explanation <- HTML("<span>&#8505;</span> Point-serial is the default correlation for ", vars, " because one is a continuous variable while the other is a categorical variable.")
      }
    return(explanation)
  }
  )
  
  # UI display 
  output$logicText <- renderUI({
    # grab caption data 
    corrLogic() 
  })
  # ======================================  Correlation Selection Options ==========================================
  
  
  # if variable A is a categorical variable, block out categorical variable options for variable B selection
  observe({
    req(input$variablesSelectedCorrA)
    codebook <- subsetCodebookNew()
    labelValues <- retrieveFilteredCodebookLabels()
    names(labelValues) <- ifelse(names(labelValues) == "Categorical Variables", "Categorical Variables (Percent)", "Continuous Variables (Percent)") 

    varAType <- codebook[codebook$Labels == input$variablesSelectedCorrA, "categoricalOrContinuous"]
    
    if (varAType == "Categorical variable") {
      print(input$variablesSelectedCorrA)
      print("var A is categorical")
      
      updatePickerInput(
        session = session, inputId = "variablesSelectedCorrB", label = "Variable B",
        choices = labelValues$`Continuous Variables`)
    }
  })
  

  # update correlation type choices based on variable types
  observe({
    shiny::validate({
      need(input$variablesSelectedCorrA, "")
      need(input$variablesSelectedCorrB, "")
    })
    
    codebook <- subsetCodebookNew()
    
    uniqueVarTypes <- unique(as.vector(codebook[codebook$Labels %in% c(input$variablesSelectedCorrA, input$variablesSelectedCorrB), "categoricalOrContinuous"]))
    varAType <- codebook[codebook$Labels == input$variablesSelectedCorrA, "categoricalOrContinuous"]
    varBType <- codebook[codebook$Labels == input$variablesSelectedCorrB, "categoricalOrContinuous"]
    
    if (length(uniqueVarTypes) == 1) {
      if (uniqueVarTypes == "Continuous variable") { # if all variables are continuous
        names = c("Spearman", "Pearson", "Polychoric", "Polyserial")
        corrChoices <- c("Spearman", "Pearson")
        disabled_choices <- !names %in% corrChoices
        updatePickerInput(
          session = session, inputId = "correlationType", label = "Correlation Type",
          choices = names, selected = "Pearson",  
          choicesOpt = list(
            disabled = disabled_choices,
            style = ifelse(disabled_choices,
                           yes = "color: rgba(119, 119, 119, 0.5);",
                           no = "")))
        print("Continuous-Continuous")
      } else { # if all variables are categorical
        corrChoices <- c("Spearman", "Pearson", "Polychoric")
        names = c("Spearman" = "Spearman", "Pearson" = "Pearson", 
                  "Polychoric" = "Polychoric", "Polyserial" = "Polyserial") 
        disabled_choices <- !names %in% corrChoices
        updatePickerInput(
          session = session, inputId = "correlationType", label = "Correlation Type",
          choices = names, selected = "Polychoric", 
          choicesOpt = list(
            disabled = disabled_choices,
            style = ifelse(disabled_choices,
                           yes = "color: rgba(119, 119, 119, 0.5);",
                           no = "")))
        print("Discrete-Discrete")
      }
    } else if (varAType == "Continuous variable") { # first var is continuous, second var is discrete
      corrChoices <- c("Spearman", "Pearson", "Polyserial")
      names = c("Spearman" = "Spearman", "Pearson" = "Pearson", 
                "Polychoric" = "Polychoric", "Polyserial" = "Polyserial")
      disabled_choices <- !names %in% corrChoices
      updatePickerInput(
        session = session, inputId = "correlationType", label = "Correlation Type",
        choices = names, selected = "Pearson", 
        choicesOpt = list(
          disabled = disabled_choices,
          style = ifelse(disabled_choices,
                         yes = "color: rgba(119, 119, 119, 0.5);",
                         no = "")))
      print("Continuous-Discrete")
    } else { # first var is discrete, second var is continuous
      corrChoices <- c("Spearman", "Pearson", "Polyserial")
      names = c("Spearman" = "Spearman", "Pearson" = "Pearson",
                "Polychoric" = "Polychoric", "Polyserial" = "Polyserial")
      disabled_choices <- !names %in% corrChoices
      updatePickerInput(
        session = session, inputId = "correlationType", label = "Correlation Type",
        choices = names, selected = "Pearson",  
        choicesOpt = list(
          disabled = disabled_choices,
          style = ifelse(disabled_choices,
                         yes = "color: rgba(119, 119, 119, 0.5);",
                         no = "")))
      print("Discrete-Continuous")
    }
  })
  
  # ============================================  create data tables  ================================================
  # when the corr table run button is clicked we build data
  corrtable_df <- eventReactive(input$obsEventButtonCorr, {
    if(input$variablesSelectedCorrA == "" | input$variablesSelectedCorrB == ""){
      return(NULL)
    }
    print(input$correlationType)

    #dynamic data 
    codebook <- subsetCodebookNew()
    selectedData <- subsetSelectedSurvey()
    varOptions <- input$selVars
    selectedVarA <- input$variablesSelectedCorrA
    selectedVarB <- input$variablesSelectedCorrB
    
    corrDataOutput <- getCorrData.TIMSS(shinyILSAData = shinyILSAData,
                                            codebook=codebook,
                                            selectedVars = varOptions,
                                            countries = input$statistic,
                                            grade = input$sample,
                                            years = sort(input$year),
                                            varADesc = selectedVarA,
                                            varBDesc = selectedVarB,
                                            method = input$correlationType,
                                            weightVar = input$weightCorrTab) #default to 'Pearson' corr method for now
    #return
    corrDataOutput
  })
  
  
  # ============================================  Cor caption  ================================================
  # when the corr table run button is clicked we build data
  # data for caption 
  # observe event so measure names don't update before a new table is generated
  captionCorDat <- eventReactive(input$obsEventButtonCorr, {
    # check for corr results 
    shiny::validate({
      need(!is.null(corrtable_df()), "")
    })
    # pull information from generated corr 
    table <- corrtable_df()
    # grade 
    grade <- unique(table[["Grade"]])
    # year
    years <- paste0(unique(table[["Year"]]), collapse = ", ")
    # vars 
    vars <- paste0("<b>",input$variablesSelectedCorrB,"</b>", ", and <b>", input$variablesSelectedCorrA, "</b>")
    # paste caption and return 
    return(HTML(paste0(input$correlationType, ' correlation between ', vars, " for TIMSS: ", grade, ", ", years)))
  }
  )
  # UI display 
  output$captionCor <- renderUI({
    # grab caption data 
    captionCorDat() 
  })
  # ============================================  Cor footnote  ================================================
  ### cor caption
  cor_nt_info <- reactive({
    table <- corrtable_df()
    # building blocks for foot note text 
    text <- "<b>SOURCE:</b> International Association for the Evaluation of Educational Achievement, Trends in International Mathematics and Science Study (TIMSS): "
    # year
    year <- paste0(paste0(unique(table[["Year"]]), collapse = ", "), " Mathmatics and Science Assessment")
    # build note, dynamic depending on missingness
    if("-" %in% table[["N"]] | "-" %in% table[["Weighted N"]])  {
      note <- "<b>NOTE:</b> Some apparent differences between estimates may not be statistically signficant."
      note <- c(note,"In case of <b>'-'</b> standards were either not met or data are not available for selected inputs.<br/>")
    } else {
      note <- "<b>NOTE:</b> Some apparent differences between estimates may not be statistically signficant.<br/>"
    }
    return(c(paste0(text, year, "."), year, note))
  })
  
  output$footnoteCor <- renderUI({
    # first generate corr table 
    shiny::validate({
      need(!is.null(corrtable_df()), "")
    })
    info <- cor_nt_info()
    text <- info[1]
    year <- info[2]
    note <- info[3]
    # define a fluid-row/column structure that includes text and image 
    # return 
    return(
      # fluid row start 
      fluidRow(
        # column text 
        column(12, align= "left", 
               HTML(note), 
               HTML(paste0(text, year, "."))
        ) ,# column text end 
        # column image 
        column(12, align="right", style = "padding-top: 25px;",
               div(#style = "align: right;",
                 tags$a(href='https://nces.ed.gov/timss/',
                        img(src="TIMSS-USA-logo.png")))
        ) # column image end 
      ) # fluid row end 
    ) # end of return 
  }
  )
  
 
  # ===========================================  filter data based on check boxes ============================================
  
  
  # function subsets the columns from the corrTab dataframe object so the rendered results table can be reshaped like a flip switch
  corrTableDetailsReactive <- reactive({
    
    shiny::validate({
      need(!is.null(corrtable_df()), "")
      need(input$variablesSelectedCorrA != "" & input$variablesSelectedCorrB != "", "Make selection(s) from all available dropdowns.")
    })

    corrTable <- corrtable_df()
    if (input$checkboxInputDetailsCorr) {
      corrTable
    } else {
      corrTable <- corrTable[,!colnames(corrTable) %in% c("Method", "Full data N", "N Used", "S.E.", "Weight")]
    }
    corrTable
  })
  
  # =======================================  render table based on final filtered data ========================================
  
  ### Corr Tab Render Table - renders the dataframe this uses the output from the corrTableDetailsReactive selection
  output$corrTableOutput <- renderTable({
    corrTableDetailsReactive()
  })
  
  
  # ==================================================  add download handler ===================================================
  
  # Corr Tab download handler
  output$downloadCorrTabData <- downloadHandler(
    filename = function() {
      paste0("CorrTable", Sys.Date(), ".csv")
    },
    content = function(file) {
      out <- corrtable_df()
      note <- cross_nt_info()[c(1,3)]
      note <- gsub('<b>|</b>|<br/>', '',note)
      out[nrow(out) + 1, 1] <- note[2]
      out[nrow(out) + 1, 1] <- note[1]
      write_csv(out, file)
    }
  )
  
  
  
########################################################################################################################
### Accordion
########################################################################################################################
  # reactive data for initial accordion choice - we want this as reactive data so we can examine selections
  accordionChoice <- reactive({

    #see dataController_TIMSS.R
    surveyKey <- paste0(input$sample, "_", paste(sort(input$year), sep = "", collapse = "&"))
    getNavAccordian.TIMSS(surveyKey)
  })
  
  # render the variable selection accordion as an UI element (based on users input of grade & year)
  output$variableSelectionAccordion <- renderUI({
    ns <- session$ns
    shiny::validate({
      need(input$sample, message = F)
      need(input$year, message = F)
      need(input$statistic, message = F)
    })

    # search box HTML
    searchBox <- '
    <div id="mainTool-variableSelectionSearchBox" class="shiny-html-output shiny-bound-output">
      <div class="form-group shiny-input-container" style="width: 450px;">
        <label for="mainTool-search">Enter your text</label>
        <div id="mainTool-search" data-reset="TRUE" data-reset-value="" class="input-group search-text shiny-bound-input">
          <input id="mainTool-search_text" style="border-radius: 0.25em 0 0 0.25em !important;" type="text" class="form-control shiny-bound-input" placeholder="Search">
          <div class="input-group-btn">
            <button class="btn btn-default btn-addon action-button shiny-bound-input" id="mainTool-search_reset" type="button">
              <i class="fa fa-remove"></i>
            </button>
            <button class="btn btn-default btn-addon action-button shiny-bound-input" id="mainTool-search_search" type="button">
              <i class="fa fa-search"></i>
            </button>
          </div>
        </div>
       </div>
     </div>'
  
  # This adds +/- to the accordion  
  accordionPlusMinus <- "
  <script type=\"text/javascript\">
    $(function ($) {
      // Initialize: prepend + / - to panel heading
      $('#mainTool-variableSelectionAccordion .panel-heading .panel-title').each(function() {
        if($(this).parent().next().hasClass('in')){
          $(this).prepend('<i class=\"glyphicon glyphicon-minus\"></i>');
        } else {
          $(this).prepend('<i class=\"glyphicon glyphicon-plus\"></i>');
        };
      });
     
      // Inner panels: switch signs when open / close
      $('#mainTool-variableSelectionAccordion .panel-collapse .collapse').on('show.bs.collapse', function () {
         $(this).prev().find('.glyphicon-plus').toggleClass('glyphicon-plus glyphicon-minus');
      });
      $('#mainTool-variableSelectionAccordion .panel-collapse .collapse').on('hide.bs.collapse', function () {
         $(this).prev().find('.glyphicon-minus').toggleClass('glyphicon-minus glyphicon-plus');
      });
      
      // Outer panels: when clicked
      $(\'[data-parent=\"#mainTool-level_1\"]\').on('click', function(e) {
        // If already opened panel is being closed: minus to plus for that panel only
        if ($(this).next().hasClass('in')) {
          $(this).find('.glyphicon-minus').toggleClass('glyphicon-minus glyphicon-plus');
        } else {
          // If closed panel is being opened
          var $selectedThis = $(this);
          // All panels to plus
          $(\'[data-parent=\"#mainTool-level_1\"]\').each(function(){
            $(this).find('.glyphicon-minus').toggleClass('glyphicon-minus glyphicon-plus');
          }); 
          // Clicked panel plus to minus
          $selectedThis.find('.glyphicon-plus').toggleClass('glyphicon-plus glyphicon-minus');  
        };
      });
    });
  </script>"
    
    tagList(
      # code for selecting variables
      tags$script(src='selectVars.js'),
      # instructions
      HTML("<h3>Instructions: </h3><h4>Select variables by navigating the accordion content section headers and clicking the variable row/checkbox. Click <i>details</i> to return more variable information. Enter terms into the <i>Search bar</i> to use variables of interest to subset the accordion.</h4><br>"),
      # search box
      HTML(searchBox),
      # count of variables
      uiOutput(ns("varCountNum")),
      # accordion and accordion +/- code
      HTML(paste0(as.character(accordionChoice()), accordionPlusMinus))
    ) 
  })

  
  output$varCountNum <- renderUI({
    varCount()
  })
  
  varCount <- reactive({
    if (is.null(input$varWordCount)) {
      HTML("")
    } else {
      if (nchar(input$searchWord) > 0) {
        HTML(paste0("Found ", input$varWordCount, " variables."))
      } else {
        HTML("")
      }
    }
  })
  

  ### Variable Selection
  # render the list of selected variables as an UI element in the sidebar area (based on users' variable selection in the accordion)
  output$variable_ui <- renderUI({
    ns <- session$ns
    
    shiny::validate({
      need(input$sample, message = F)
      need(input$year, message = F)
      need(input$statistic, message = F)
    })

    tagList(
         h3("Variables"),
         wellPanel(uiOutput(ns("nodes_global")))
     )
  })

  output$nodes_global <- renderUI({
    getNodes()
  })
  
 # reset selected vars if value for year or sample is changed
 observeEvent(input$sample, {
   session$sendCustomMessage("reSet", "reset");
  })
 observeEvent(input$year, {
   session$sendCustomMessage("reSet", "reset");
 })

  # selected variable inputs come from selectVars.js
  getNodes <- reactive({
    if (is.null(input$selVars)) {
      HTML("")
    } else {
      # get variable type and paste to selections 
      codebook <- subsetCodebookNew()
      selected <- input$selVars
      codebookFiltered <- codebook %>% filter(ID %in% selected)
      codebookFiltered2 <<- codebook
      selectedLbl <- paste(codebookFiltered[,10], paste0("[",codebookFiltered[,15],"]"))
      # continue with javascript
      nodes <- paste0('<li><label><input type="checkbox" class="sideCheck" value="', selectedLbl, '" checked/> ',selectedLbl,' </label></li>')
      nodes_ui <- paste0('<ul class="list-unstyled">', nodes, '</ul>
                      <script type="text/javascript">
                        $(function() {
                          // unchecking will make variable disappear
                          $("input.sideCheck[type=checkbox]").on("click", function() {
                            if($(this).prop("checked")===false){
                              $(this).parent().remove()
                              var vraw = this.value;
                              var v = vraw.split(\' [\')[0];
                              $("input:checkbox[value=\'\"+v+\"\']").prop("checked", false);
                              selectedVars = selectedVars.filter(function(item) {
                                return item !== v;
                              });
                              Shiny.setInputValue("mainTool-selVars", selectedVars);
                            }
                          });
                          
                          // scrolling over will fetch the variable text from accordion
                          $(".list-unstyled li label").on("mouseenter", function() {
                              let findIdraw = $.trim($(this).text());
                              let findId = findIdraw.split(\' [\')[0];
                              let spanText = $("#mainTool-level_1 :input[value=\'\"+ findId +\"\' ]").next().contents().filter(function() {
                                              return this.nodeType == Node.TEXT_NODE;
                                            }).get(0).nodeValue;
                              $(this).attr("title", spanText);
                          });
                        });
                      </script>')
      HTML(nodes_ui)
    }
  })
} # end 'plotTable' function
)}
