
dedupe <- function(r) {
  makeReactiveBinding("val")
  observe(val <<- r(), priority = 10)
  reactive(val)
}


invalidInput <- function(input) {
  return(is.null(input) || input == "")
}


### add themes
theme_general <- theme(text=element_text(family="Open Sans", color = "#656565"),
                     panel.background=element_blank(),
                     panel.border=element_rect(color="transparent"),
                     plot.margin = unit(c(.5, 2, .5, .5), "cm"),
                     panel.grid.major.y=element_blank(),
                     panel.grid.major.x=element_blank(),
                     panel.grid.minor.x=element_blank(),
                     panel.grid.minor.y=element_blank(),
                     axis.title.y = element_text(color = "black",size=13, family = "Open Sans Semibold"),
                     axis.title.x = element_text(color = "black",size=13, family = "Open Sans Semibold"),
                     axis.line.x=element_line(),
                     axis.line.y=element_line(),
                     axis.text.x=element_text(color="black",size=10),
                     axis.text.y=element_text(color="black",size=10),
                     axis.ticks.x=element_blank(),
                     axis.ticks.y=element_blank(),
                     plot.title=element_text(size=16,lineheight=1.15, color="#808184"),
                     plot.subtitle=element_text(color="black",size=12, margin = margin(t=10, b= 5)),
                     plot.caption=element_text(color="black",size=10, hjust=0, margin=margin(t=15),lineheight=1.15),
                     #axis.ticks.length =  unit(.15, "cm"),
                     legend.position="bottom",
                     legend.text.align = 0,
                     legend.text = element_text(color="black", size = 10),
                     legend.margin = margin(0, 0, 0, 0),
                     legend.spacing = unit(c(1), "line"),
                     legend.title = element_blank(),
                     legend.key.height=unit(1.5,"line"),
                     legend.key.width=unit(1.5,"line"),
                     legend.spacing.x = unit(.2, 'cm'),
                     strip.background = element_blank(),
                     strip.text=element_text(color="black",size=10, margin = margin(t = 0, r = 0, b = 0, l = 10)),
                     panel.spacing = unit(2, "lines")
)

myDownloadButton <- function(outputId, label = "Download",style=style){
  tags$a(id = outputId, class = "btn btn-default shiny-download-link", href = "", 
         target = "_blank", download = NA, NULL, label,style=style)
}


## functions for bsplu accordions UI

create_L3 <- function(i, l1VariableLabel, l2VariableLabel, data){
  isContinuous <- data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(categoricalOrContinuous) %>% as.character() == "Continuous variable"
  desIsNA <- data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(des) %>% as.character() %>% is.na()
  
  HTML(paste0("<b>[", data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(ID) %>% as.character(), "]</b>",
              " ",
    data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(Labels) %>% as.character(), 
              " ",
              tags$a("details") %>% 
                bs_attach_collapse(id = gsub('([[:punct:]])|\\s+','_', paste0(l1VariableLabel,"_",l2VariableLabel, "_", i))),
              bs_collapse(
                id = gsub('([[:punct:]])|\\s+','_', paste0(l1VariableLabel,"_",l2VariableLabel, "_", i)), 
                content = tags$div(class = "well", 
                                          strong("Type:"), data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(categoricalOrContinuous) %>% as.character(),
                                          br(),
                                          #if the "des" column is NA (which is true for most continuous variables), hide "Description". this may need to updated later
                                          strong(ifelse(test = desIsNA,
                                                        yes = "",
                                                        no = "Description:"
                                          )),
                                          ifelse(test = desIsNA,
                                                   yes = "",
                                                   no = data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(des) %>% as.character()),
                                          ifelse(test = desIsNA,
                                                         yes = "",
                                                         no = list(br())
                                          ),
                                   
                                          strong("Year Availability:"),  data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(yearAvailability) %>% as.character(),
                                          br(),
                                   
                                          strong("ID:"),  data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(ID) %>% as.character(),
                                          br(),
                                          
                                          # for categorical variables
                                          strong(ifelse(test = isContinuous,
                                                         yes = "",
                                                         no = "Values:"
                                          )),
                                          ifelse(test = isContinuous,
                                                  yes = "",
                                                  no = data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% slice(i) %>% select(labelValues) %>% as.character()),
                                 )
                  
                
              )))
  
}



create_L2 <- function(tag, l2VariableLabel, l1VariableLabel, data, ns){
  checkGroupInputID = ns(gsub('([[:punct:]])|\\s+','_', paste0(l1VariableLabel,"_",l2VariableLabel)))
  bs_append(tag,
            title = l2VariableLabel, 
            content = checkboxGroupInput(inputId = checkGroupInputID, label = "Variables",
                                         choiceNames = purrr::map(c(1:nrow((data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel)))),create_L3, l1VariableLabel = l1VariableLabel, l2VariableLabel = l2VariableLabel, data = data),
                                         choiceValues = strsplit(data %>% filter(L1 == l1VariableLabel & L2 == l2VariableLabel) %>% select(ID) %>% pull(), "/,"),
                                         width = "100%"
            ))
}


create_L1 <- function(tag, l1VariableLabel, data, ns){
  bs_append(tag,
            title = l1VariableLabel,
            content =  purrr::reduce(
              .x = data %>% filter(L1 == l1VariableLabel) %>% select(L2) %>% unique() %>% pull(),
              .f = create_L2,
              l1VariableLabel = l1VariableLabel,
              data = data,
              ns = ns,
              .init = bs_accordion(id = ns(gsub('([[:punct:]])|\\s+','-', paste0("level_2_",l1VariableLabel)))) %>% 
                bs_set_opts(panel_type = "default", use_heading_link = TRUE)
            ) 
  )
}

create_accordion <- function(data, ns){
  purrr::reduce(
    .x = data %>% select(L1) %>% unique() %>% pull(),
    .f = create_L1,
    data = data,
    ns = ns,
    .init = bs_accordion(id = ns("level_1")) %>%
      bs_set_opts(panel_type = "primary", use_heading_link = TRUE)
  )
}

## functions for bsplu accordions Server

get_L1_L2_IDs <- function(l2VariableLabel, l1VariableLabel){
  gsub('([[:punct:]])|\\s+','_', paste0(l1VariableLabel,"_",l2VariableLabel))
}

get_L2_IDs <- function(l1VariableLabel, data){
  L2_vars <- data %>% filter(L1 == l1VariableLabel) %>% select(L2) %>% unique() %>% pull()
  purrr::map(L2_vars, get_L1_L2_IDs, l1VariableLabel)
}

create_accordion_id <- function(data){
  
  L1_vars <- data %>% select(L1) %>% unique() %>% pull()
  
  purrr::map(L1_vars, get_L2_IDs, data) %>% unlist()
}

# add a function to help insert multiple line breaks
linebreaks <- function(n){HTML(strrep(br(), n))}
