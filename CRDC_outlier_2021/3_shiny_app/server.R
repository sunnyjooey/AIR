library(ggplot2)
library(ggtext)
library(tidyr)
library(DT)
library(writexl)
library(bcrypt)

options(scipen=999)

#### data
all_data <- readRDS('data/all_dfs.rds')
all_mod_z <- readRDS('data/all_diffs.rds')
all_neigh <-readRDS('data/all_neigh.rds')
mod <- readRDS('data/mod_col.rds')


#### authorization
# pw <- scrypt::hashPassword("")
pw <- "c2NyeXB0ABAAAAAIAAAAAXnoo3QCmgSBJ0Fl8/1TKeG75c4m6xEOqul4+zCZboolkhKeFsSppQiBion/6VCmwou5YOu8udbzEcM72J/WQZfd9uParG0uWA+vLBQloQU1"
credentials <- data.frame(
  user = c("crdc"),
  password = pw,
  is_hashed_password = T,
  stringsAsFactors = FALSE
)


#### main function
server <- function(input, output, session) {
  
  # authorization
  result_auth <- secure_server(check_credentials = check_credentials(credentials))
  
  ## Timeout
  observeEvent(input$timeOut, { 
    print(paste0("Session (", session$token, ") timed out at: ", Sys.time()))
    showModal(modalDialog(
      title = "Timeout",
      paste("Session timeout due to", input$timeOut, "inactivity -", Sys.time()),
      footer = NULL
    ))
    session$close()
  })

  ###################### BY SCHOOL
  # row in the dataframe (ordered from most to least outlierly)
  # region's raw data
  data <- reactive({
    # wait until valid user
    req(result_auth$user)
    data <- all_data[[input$region]]
    return(data)
  })
  
  # z-scores by module of region
  mod_z <- reactive({
    req(input$region)
    mod_z <- all_mod_z[[input$region]]
    return(mod_z)
  })
  
  # neighbor ids
  neigh <- reactive({
    n <- all_neigh[[input$region]]
    return(n)
  })
  
  # school selection dropdown
  output$obs_UI <- renderUI({
    input$region
    selectInput(inputId = "obs",
                label = HTML('Choose Outlier School<br/><font size="-2">Start typing to search schools</font>'),
                choices = c())
  })
  
  observeEvent(input$region,
               {
                 data <- all_data[[input$region]]
                 schools <- paste0(1:nrow(data), ' - ', data$sch_name, ' (', data$state, ', ', data$leaid, ')')
                 updateSelectInput(session, "obs",
                                   choices = schools)
               }
  )
  
  obsId <- reactive({
    shiny::validate({
      need(input$region, message = FALSE)
    })
    input$obs
  })

  
  # outlier modules table
  obs_z <- reactive({
    if (length(input$obs) > 0) {
      obs <- obsId()
      obs <- as.numeric(unlist(strsplit(obs,' '))[1])
    } else {
      obs <- 1
    }
    mod_z <- mod_z()
    z <- as.numeric(mod_z[obs,])
    z <- data.frame(list(
      Module=colnames(mod_z),
      Score=round(z,2)
    ))
    return(z)
  })
  
  # render the outlier module table
  output$table1 <- DT::renderDataTable(
    datatable(obs_z(), selection=list(mode='single'), options = list(pageLength = nrow(obs_z())))
  ) 
  
  # Which row in the outlier table needs to be rendered in the graph
  s_row <- reactiveValues(index = NULL)
  
  # find min score
  observe({
    if (!is.null(input$table1_rows_selected)) {
      # row/module has been selected
      s_row$index <- input$table1_rows_selected
    } else {
      di <- obs_z()[1:length(names(mod)),]
      mn <- which.min(di$Score)
      s_row$index <- mn
    }
  })
  
  # function to determine plot height
  getHeight <- function(s_row_i) {
    if (!is.null(s_row_i)) {
      one_mod <- obs_z()[s_row_i, "Module"]
      if (length(one_mod) == 0){
        h <- 0
      } else if (length(mod[[one_mod]]) < 16) {
        h <- 500
      } else if (length(mod[[one_mod]]) < 30) {
        h <- 1000
      } else if (length(mod[[one_mod]]) < 80) {
        h <- 3000
      } else {
        h <- 4500
      }
    } else {
      h <- 500
    }
    return(h)
  }
  
  # main graph
  output$view <- renderPlot({
    input$table1
    # data for region
    data <- data()
    if (nrow(data) >0 ) {
      # the school
      obs <- obsId()
      obs <- as.numeric(unlist(strsplit(obs,' '))[1])
      
      # school's zs by module
      one_mod <- obs_z()[s_row$index, "Module"]
      # school's neighbors' indices
      neigh_ind <- unlist(neigh()[obs,])
      # chosen module
      mod_col <- mod[[one_mod]]
      mod_col <- mod_col[!mod_col %in% 'tot_enrl']
      # neighbors' data
      neigh_df <- data[neigh_ind,c(mod_col,'tot_enrl')]
      neigh_mean <- apply(neigh_df,2,mean, na.rm=T)
      neigh_norm <- neigh_mean / neigh_mean[['tot_enrl']]
      # schools' data
      outlier <- data[obs,c(mod_col,'tot_enrl')]
      outlier_norm <- outlier/outlier[['tot_enrl']]
      # region's data
      no_obs <- which(1:nrow(data) != obs)
      region_mean <- apply(data[no_obs,c(mod_col, "tot_enrl")],2,mean, na.rm=T)
      region_norm <- region_mean /region_mean[['tot_enrl']]
      # clean
      names(neigh_norm) <- NULL
      names(outlier_norm) <- NULL
      names(region_norm) <- NULL
      rownames(outlier_norm) <- NULL
      outlier_norm <- unlist(c(outlier_norm))
      names(outlier) <- NULL
      outlier <- as.numeric(outlier)
      # reshape
      pre_graph <- data.frame('Region_avg'=region_norm[1:length(mod_col)], 'Neighbor_avg'=neigh_norm[1:length(mod_col)], 'Outlier_school'=outlier_norm[1:length(mod_col)], 'out_raw'=outlier[1:length(mod_col)], Variables=1:length(mod_col))
      graph_df <- tidyr::pivot_longer(pre_graph, cols=c('Region_avg', 'Neighbor_avg', 'Outlier_school', 'out_raw'), names_to='Category', 
                                      values_to="Normalized")
      graph_df$Category <- factor(graph_df$Category, levels=c('Region_avg','Neighbor_avg', 'Outlier_school', 'out_raw'))
      # get nearest 10
      x <- max(graph_df[graph_df$Category!='out_raw','Normalized'], na.rm = T)
      rounded10 <- 10^ceiling(log10(x))
      if(x < rounded10/2) {
        rounded10 <- rounded10/2
      }
      
      # graph
      ggplot(graph_df) +
        geom_bar(data=subset(graph_df, Category!='out_raw'), aes(x=Variables, y=Normalized, fill=Category), stat='identity', width=0.4, position = position_dodge(width=0.4)) +
        scale_x_continuous(breaks=1:length(mod[[one_mod]]), labels=mod[[one_mod]])+
        scale_fill_manual(values = c("#E69F00", "#56B4E9", "#009E73","black"))+
        theme(axis.text.y = element_text(size = 12) )+
        geom_text(
          data=subset(graph_df, Category=='out_raw'),
          aes(label = Normalized, x=Variables, y = Normalized/outlier[length(outlier)]+(rounded10*.03)),
          position = position_dodge(0.9),
          vjust = 0
        ) +
        ylim(0,rounded10) +
        coord_flip()+
        labs(caption=paste0('Region avg enrollment: ', round(region_mean[length(region_mean)]), '\n',
                            'Neighbor avg enrollment: ', round(neigh_mean[length(neigh_mean)]), '\n',
                            'Outlier school enrollment: ', data[obs,'tot_enrl'])) +
        ggtitle(paste0("<p><span style='font-size:18pt'>Module: ", one_mod, " </span></p><p><span style='font-size:12pt'>*The numbers displayed refer to student counts in the outlier school</span></p>")) +
        theme(plot.caption = element_text(hjust = 0, size = 12),
              plot.title = element_markdown(),
              legend.justification = "top")
    } else {
      ggplot()
    }
  }, height = function(x) getHeight(s_row$index))
  
  # render the main graph
  output$plot <- renderUI({
    shiny::validate({
      need(input$obs, message = FALSE)
    })
    plotOutput("view", height = "auto")
  })
  
  school <- reactive({
    if (length(input$obs) > 0) {
      obs <- obsId()
      obs <- as.numeric(unlist(strsplit(obs,' '))[1])
    } else {
      obs <- 1
    }
    schl <- all_data[[input$region]]$sch_name[obs]
    s <- paste0("Likely Outlier Module for ", schl, ": ")
    return(s)
  })
  
  # title shows most probable module
  output$title_panel <- renderText({
    di <- obs_z()[1:length(names(mod)),]
    mn <- which.min(di$Score)
    one_mod <- obs_z()[mn, "Module"]
    paste0(school(),  one_mod, "\n")
  })
  
  # download school
  download_school <- reactive({
    data <- data()
    # the school
    obs <- obsId()
    obs <- as.numeric(unlist(strsplit(obs,' '))[1])
    
    # school's zs by module
    one_mod <- obs_z()[s_row$index, "Module"]
    # school's neighbors' indices
    neigh_ind <- unlist(neigh()[obs,])
    # chosen module
    mod_col <- mod[[one_mod]]
    mod_col <- mod_col[!mod_col %in% 'tot_enrl']
    # neighbors' data
    neigh_df <- data[neigh_ind,c(mod_col,'tot_enrl')]
    neigh_mean <- apply(neigh_df,2,mean, na.rm=T)
    neigh_norm <- neigh_mean / neigh_mean[['tot_enrl']]
    # schools' data
    school_raw <- data[obs,c(mod_col,'tot_enrl')]
    school_norm <- school_raw/school_raw[['tot_enrl']]
    # region's data
    no_obs <- which(1:nrow(data) != obs)
    region_mean <- apply(data[no_obs,c(mod_col, "tot_enrl")],2,mean, na.rm=T)
    region_norm <- region_mean /region_mean[['tot_enrl']]
      
    # put together
    prep_df <- rbind(school_raw, school_norm, neigh_mean, neigh_norm, region_mean, region_norm)  
    prep_df <- round(prep_df, 4)
    name <- data[obs, 'sch_name']
    state <- data[obs, 'state']
    lea <- data[obs, 'leaid']
    kind_df <- data.frame('0'=c('school_raw', 'school_norm', 'neighbors_mean', 'neighbors_norm', 'region_mean', 'region_norm'))
    info_df <- data.frame(school=c(name, name, 'NA', 'NA', 'NA', 'NA'), state=c(state, state, 'NA', 'NA', 'NA', 'NA'), leaid=c(lea, lea, 'NA', 'NA', 'NA', 'NA'))
    one_df <- cbind(kind_df, info_df, prep_df)
       
    return(one_df)
  })
  
  sname <- reactive({
    n <- trimws(unlist(strsplit(obsId(),'-'))[2])
    n <- trimws(unlist(strsplit(n, '\\(')[1]))
    return(n)
  })
  
  # call download
  output$download1 <- downloadHandler(
    filename = function() {paste0(input$region, '_', sname(), '_', obs_z()[s_row$index, "Module"], '.xlsx')},
    content = function(file) {write_xlsx(download_school(), path = file)}
  )
  
  
  ######################### BY MODULE
  modbase <- reactive({
    input$modbase
  })
  
  # region's raw data
  data2 <- reactive({
    data <- all_data[[input$region2]]
    return(data)
  })
  
  # z-scores by module of region
  mod_z2 <- reactive({
    mod_z <- all_mod_z[[input$region2]]
    data <- data2()
    schools <- paste0(data$sch_name, ' (', data$state, ', ', data$leaid, ')')
    mod_z <- mod_z[,modbase()]
    z <- data.frame(list(School = schools,
              Score = mod_z))
    z <- z[order(z$Score),]
    z$Score <- round(z$Score,2)
    return(z)
  })
  
  # neighbors data
  neigh2 <- reactive({
    n <- all_neigh[[input$region2]]
    return(n)
  })
  
  # get rid of rownames in mod_z2
  mod_z2_ <- reactive({
    mod_z2_ <- mod_z2()
    rownames(mod_z2_) <- NULL
    return(mod_z2_)
  })
  
  # render the outlier module table
  output$table2 <- DT::renderDataTable(
    datatable(mod_z2_(), selection='single', options = list(pageLength = 25))
  ) 
  
  # Which row in the outlier table needs to be rendered in the graph
  s_row2 <- reactiveValues(index = NULL)
  
  observe({
    if (!is.null(input$table2_rows_selected)) {
      # row/module has been selected
      s_row2$index <- input$table2_rows_selected
    } else {
      s_row2$index <- 1
    }
  })
  
  # function to determine plot height
  getHeight2 <- function() {
      one_mod <- modbase()
      if (length(one_mod) == 0){
        h <- 0
      } else if (length(mod[[one_mod]]) < 16) {
        h <- 500
      } else if (length(mod[[one_mod]]) < 30) {
        h <- 1000
      } else if (length(mod[[one_mod]]) < 80) {
        h <- 3000
      } else {
        h <- 4500
      }
    return(h)
  }
  
  # main graph
  output$view2 <- renderPlot({
    data <- data2()  # all data
    one_mod <- modbase()  # the module
    obs <- as.numeric(rownames(mod_z2())[s_row2$index])  # one school
    school_name <- mod_z2()[s_row2$index, 1]  # the school name
    school_name <- gsub('/', ' ', school_name)
    neigh_ind <- unlist(neigh2()[obs,])  # the school's neighbor's index
    mod_col <- mod[[one_mod]]  # column names in module
    mod_col <- mod_col[!mod_col %in% 'tot_enrl']  
    neigh_df <- data[neigh_ind,c(mod_col,'tot_enrl')]  # neighbors' data for module
    neigh_mean <- apply(neigh_df,2,mean, na.rm=T)  # neighbors' data mean
    neigh_norm <- neigh_mean / neigh_mean[['tot_enrl']]  # neighbors' data div by enrl
    outlier <- data[obs,c(mod_col,'tot_enrl')]  # school's data for module
    outlier_norm <- outlier/outlier[['tot_enrl']]  # school's data div by enrl
    no_obs <- which(1:nrow(data) != obs)  # num obs in region 
    region_mean <- apply(data[no_obs,c(mod_col, "tot_enrl")],2,mean, na.rm=T)  # region data mean
    region_norm <- region_mean /region_mean[['tot_enrl']]  # region data div by enrl
    names(neigh_norm) <- NULL
    names(outlier) <- NULL
    names(region_norm) <- NULL
    rownames(outlier_norm) <- NULL
    outlier_norm <- unlist(c(outlier_norm))
    names(outlier) <- NULL
    outlier <- as.numeric(outlier)
    pre_graph <- data.frame('Region_avg'=region_norm[1:length(mod_col)], 'Neighbor_avg'=neigh_norm[1:length(mod_col)], 'Outlier_school'=outlier_norm[1:length(mod_col)], 'out_raw'=outlier[1:length(mod_col)], Variables=1:length(mod_col))
    graph_df <- tidyr::pivot_longer(pre_graph, cols=c('Region_avg', 'Neighbor_avg', 'Outlier_school', 'out_raw'), names_to='Category',
                               values_to="Normalized")
    graph_df$Category <- factor(graph_df$Category, levels=c('Region_avg','Neighbor_avg', 'Outlier_school', 'out_raw'))
    x <- max(graph_df[graph_df$Category!='out_raw','Normalized'])
    rounded10 <- 10^ceiling(log10(x))
    if(x < rounded10/2) {
      rounded10 <- rounded10/2
    }

    ggplot(graph_df) +
      geom_bar(data=subset(graph_df, Category!='out_raw'), aes(x=Variables, y=Normalized, fill=Category), stat='identity', width=0.4, position = position_dodge(width=0.4)) +
      scale_x_continuous(breaks=1:length(mod[[one_mod]]), labels=mod[[one_mod]])+
      scale_fill_manual(values = c("#E69F00", "#56B4E9", "#009E73","black"))+
      theme(axis.text.y = element_text(size = 12) )+
      geom_text(
        data=subset(graph_df, Category=='out_raw'),
        aes(label = Normalized, x=Variables, y = Normalized/outlier[length(outlier)]+(rounded10*.03)),
        position = position_dodge(0.9),
        vjust = 0
      ) +
      ylim(0,rounded10) +
      coord_flip()+
      labs(caption=paste0('Region avg enrollment: ', round(region_mean[length(region_mean)]), '\n',
                          'Neighbor avg enrollment: ', round(neigh_mean[length(neigh_mean)]), '\n',
                          'Outlier school enrollment: ', data[obs,'tot_enrl']))+
      ggtitle(paste0("<p><span style='font-size:18pt'>School: ", school_name, " </span></p><p><span style='font-size:12pt'>*The numbers displayed refer to student counts in the outlier school</span></p>")) +
      theme(plot.caption = element_text(hjust = 0, size = 12),
            plot.title = element_markdown(),
            legend.justification = "top")
  }, height = function(x) getHeight2())
  
  
  # render the main graph
  output$plot2 <- renderUI({
    plotOutput("view2", height = "auto")
  })
  
  # title which module
  output$title_panel2 <- renderText({
    paste0("Module: ", modbase(), "\n")
  })
  
  # download cases: make a sheet for each school
  download_cases <- reactive({
    nums <- as.numeric(unlist(strsplit(input$download_numbers, '-')))  # num cases, first and last number
    data <- data2()  # all data
    one_mod <- modbase()  # the module
    
    if (is.na(nums) | length(nums) > 2 | length(nums) == 0) {
      return(list())
    } else if (length(nums) == 2) {
      start <- nums[1]
      end <- min(nrow(mod_z2()), nums[2])
      schools <- mod_z2()[start:end, ]  # the schools
    } else {
      num <- min(nrow(mod_z2()), nums)
      schools <- mod_z2()[num, ]  # the school
    }
    
    df_lst <- list()

    for (i in 1:nrow(schools)) {
      obs <- as.numeric(row.names(schools[i, ]))  # row num of school
      neigh_ind <- unlist(neigh2()[obs,])  # the school's neighbors' indices
      mod_col <- mod[[one_mod]]  # column names in module
      mod_col <- mod_col[!mod_col %in% 'tot_enrl']  
      neigh_df <- data[neigh_ind,c(mod_col,'tot_enrl')]  # neighbors' data for module
      neighbors_mean <- apply(neigh_df,2,mean, na.rm=T)  # neighbors' data mean
      neighbors_norm <- neighbors_mean / neighbors_mean[['tot_enrl']]  # neighbors' data div by enrl
      school_raw <- data[obs,c(mod_col,'tot_enrl')]  # school's data for module
      school_norm <- school_raw/school_raw[['tot_enrl']]  # school's data div by enrl
      no_obs <- which(1:nrow(data) != obs)  # num obs in region 
      region_mean <- apply(data[no_obs,c(mod_col, "tot_enrl")],2,mean, na.rm=T)  # region data mean
      region_norm <- region_mean /region_mean[['tot_enrl']]  # region data div by enrl
      
      # put together
      prep_df <- rbind(school_raw, school_norm, neighbors_mean, neighbors_norm, region_mean, region_norm)  
      prep_df <- round(prep_df, 4)
      name <- data[obs, 'sch_name']
      name <- gsub('/', ' ', name)
      state <- data[obs, 'state']
      lea <- data[obs, 'leaid']
      kind_df <- data.frame('0'=c('school_raw', 'school_norm', 'neighbors_mean', 'neighbors_norm', 'region_mean', 'region_norm'))
      info_df <- data.frame(school=c(name, name, 'NA', 'NA', 'NA', 'NA'), state=c(state, state, 'NA', 'NA', 'NA', 'NA'), leaid=c(lea, lea, 'NA', 'NA', 'NA', 'NA'))
      one_df <- cbind(kind_df, info_df, prep_df)
      df_lst[[name]] <- one_df      
    }
    return(df_lst)
  })
  
  # call download
  output$download2 <- downloadHandler(
    filename = function() {paste0(input$region2, '_', modbase(), '_', trimws(input$download_numbers), '.xlsx')},
    content = function(file) {write_xlsx(download_cases(), path = file)}
  )
}


