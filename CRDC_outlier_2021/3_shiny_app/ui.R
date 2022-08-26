library(shinyBS)
library(shinymanager)
library(scrypt)


# data
regs <- c("Atlanta", "Seattle", "Denver", "Kansas City", "San Fransisco", "Boston", "Philadelphia", "Chicago", "Dallas", "Cleveland", "New York", "DC")
mod <- readRDS('data/mod_col.rds')

options(warn=-1)


# logout in case of inactivity
timeoutSeconds <- 300
inactivity <- sprintf("function idleTimer() {
var t = setTimeout(logout, %s);
window.onmousemove = resetTimer; // catches mouse movements
window.onmousedown = resetTimer; // catches mouse movements
window.onclick = resetTimer;     // catches mouse clicks
window.onscroll = resetTimer;    // catches scrolling
window.onkeypress = resetTimer;  //catches keyboard actions

function logout() {
Shiny.setInputValue('timeOut', '%ss')
}

function resetTimer() {
clearTimeout(t);
t = setTimeout(logout, %s);  // time is in milliseconds (1000 is 1 second)
}
}
idleTimer();", timeoutSeconds*1000, timeoutSeconds, timeoutSeconds*1000)



               
# Define UI for dataset viewer app ----
ui <- secure_app(head_auth = tags$script(inactivity),
  fluidPage(
  tags$head(HTML("<title>CRDC Outlier Detection Tool</title>")),  
  tags$script(inactivity), 
  tags$head(tags$style("#title_panel{font-size: 20px;}")),
  
  # Sidebar layout with a input and output definitions ----
  tabsetPanel(
    tabPanel("By School", fluid = TRUE,
                       sidebarLayout(
                         sidebarPanel(
                           selectInput(inputId = "region",
                                       label = "Choose OCR Region",
                                       choices = regs),

                           uiOutput("obs_UI"),
                           
                           DT::DTOutput("table1"),
                           bsTooltip("table1", "A lower Score indicates greater outlierness of the module for the school.",
                                     "top", options = list(container = "body")),
                           br(),
                           downloadButton(
                             "download1",
                             "Download Data to Excel"
                           ),
                           bsTooltip("download1", "Download the selected module data for the school.",
                                     "top", options = list(container = "body")),
                         ),
                         mainPanel(
                           titlePanel(h3(textOutput("title_panel"))),
                           uiOutput("plot"),
                           img(src = "AIR_CMYK4.jpg", height = 60)
                         )
                       )
    ),
    tabPanel("By Module", fluid = TRUE,
             sidebarLayout(
               sidebarPanel(
                 selectInput(inputId = "region2",
                             label = "Choose OCR Region",
                             choices = regs),
                 
                 selectInput(inputId = "modbase",
                             label = "Choose Module",
                             choices = names(mod)),
                 
                 DT::DTOutput("table2"),
                 bsTooltip("table2", "A lower Score indicates greater outlierness of the module for the school. Schools have been listed in ascending order.",
                           "top", options = list(container = "body")),
                 br(),
                 textInput("download_numbers", "Download Cases", "1 - 20"),
                 downloadButton(
                   "download2",
                   "Download Data to Excel"
                 ),
                 bsTooltip("download2", "Indicate a range (ex. 1-20) or an index number (ex. 10) from the table above to download. A sheet will be created for each school. The last entry will be taken if you go beyond the range of the table. Any other input will download an empty file.",
                           "top", options = list(container = "body")),
               ),
               mainPanel(
                 titlePanel(h3(textOutput("title_panel2"))),
                 uiOutput("plot2"),
                 img(src = "AIR_CMYK4.jpg", height = 60)
               )
             )
      )
    )
  )
)