source("uiPack.R",local = TRUE)

shinyUI(fluidPage(id = "my-nav",
  includeCSS("www/style.css"),
  shinyjs::useShinyjs(),
  tags$head(
    tags$link(rel = "stylesheet", type = "text/css", href = 'https://fonts.googleapis.com/css?family=Open Sans'),
    tags$link(rel = "stylesheet", type = "text/css", href = "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"),
    tags$link(rel="icon", href="favicon.ico"),
    tags$script(src = "stickyfill.min.js"),
    tags$script(src = "custom.js"),
         div(id = "hidden", style="padding: 0px 0px; margin-top: 0px; margin-bottom: 0px; width: '100%'",
         titlePanel(
                title="", windowTitle= "InCompass"
         )
      )
  ),
  # theme = shinytheme("paper"),
  div(
    id = "main_content",
  # navbarPage(HTML(paste0(span(img(src="compass.svg"), a("InCompass", href="#", style="color: white;")))),
  # HTML(paste0(span(img(src="compass.svg"), actionLink(inputId = "title", label = "InCompass", style="color: white;"))))
  navbarPage(HTML(paste0(span(img(src="compass.svg"), a("InCompass", href="#", style="color: white; text-decoration : none !important;")))),
             # Tab 1: Home
             tabPanel("About", value = "homePage",
                      homeUI()
             ),
             # Tab 2: TIMSS analytics
             tabPanel("TIMSS", value = "timssApp",
                      mainToolUI()
                      )
             )
  )
))



