library(shiny)
library(shinydashboard)
library(bsplus)
library(datasets)
library(purrr)


CO2_factor_levels <- list(Plant = c("Qn1", "Qn2", "Qn3", "Qc1", "Qc3", "Qc2", "Mn3", "Mn2", "Mn1", 
                                    "Mc2", "Mc3", "Mc1"),
                          Type = c("Quebec", "Mississippi"),
                          Treatment = c("nonchilled", "chilled"))

# CO2_factor_levels <- list(Test1 = c("Plant"),
#                           Test2 =  c("Type"),
#                           Test3 = c("Treatment"))



fn_append <- function(tag, title, choices) {
  bsplus::bs_append(
    tag,
    title = title,
    content = checkboxGroupInput(
      inputId = paste0("t-", title),
      label = NULL,
      choices = choices
    )
  )
}


accordion_wrapper <- function(tag, title, reduce2) {
  bsplus::bs_append(
    tag,
    title = title,
    content = reduce2
  )
}

# Define UI for application
ui <- fluidPage(theme = "style.css",
  
#   # starting with an initial accordion-tag, append panels according to
#   # list of factor levels

   # reduce2(
   #      names(CO2_factor_levels),
   #      lapply(CO2_factor_levels, function(x) paste0("HTML('<strong>')", x, "HTML('</strong>')")),
   #      fn_append,
   #      .init = bs_accordion(id = "test2")
   #    )
    
    

  bs_accordion(id = "meet_the_beatles2") %>%
    bs_set_opts(panel_type = "default", use_heading_link = TRUE) %>%
    bs_append(title = "John Lennon2", content = bs_accordion(id = "meet_the_beatles") %>%
                bs_set_opts(panel_type = "default", use_heading_link = TRUE) %>%
                bs_append(title = "John Lennon", content =   checkboxGroupInput("variable", "Variables to show:",
                                                                                c("Cylinders" = "cyl",
                                                                                  "Transmission" = "am",
                                                                                  "Gears" = "gear"))) %>%
                bs_set_opts(panel_type = "default") %>%
                bs_append(title = "Paul McCartney", content = checkboxGroupInput("variable", "Variables to show:",
                                                                                   c("Cylinders" = "cyl",
                                                                                     "Transmission" = "am",
                                                                                     "Gears" = "gear"))))
)

# Define server logic
server <- function(input, output) {
  
}

# Run the application
shinyApp(ui = ui, server = server)