

# Home Overview Tab 
homeUI = function(nav = "shinyILSA Home", tab = "shinyILSA Home") tagList(
  #tags$script('var selectedVars = [];'),
  fluidRow(
    column(8,  
           # title 
           HTML("<p style = \"font-weight: 600; color: #45458f; font-size: 17px;\"> What is InCompass</p>"),
           # text 
           HTML("<font color = \"#45458f\"><i>InCompass</i></font> is a free online tool that makes it easy to explore and analyze international large-scale assessment (ILSA) data. InCompass has data from the following studies built in so you can dive right in!"), 
           # bullets
           # do bullets for ease of adding actionLink to switch between tabs
           tags$ul(
             tags$li(actionLink(inputId = 'timssApp', label = HTML('<font color = blue><u>Trends in International Mathematics and Science Study (TIMSS)</u></font>'))), 
             tags$li("More to be added")
             
           ), 
           # title 
           HTML("<p style = \"font-weight: 600; color: #45458f; font-size: 17px;\"> What Can I do with InCompass </p>"),
           # bullets 
           HTML("<ul><li>Explore data for thousands of variables</li><li>Create weighted and unweighted descriptive statistics</li><li>Produce descriptive tables including conditional means and percentages</li><li>Conduct correlation analyses</li><li>More to be added</li></ul>"), 
           # title 
           HTML("<p style = \"font-weight: 600; color: #45458f; font-size: 17px;\"> Learn More </p>"),
           # text 
           HTML("InCompass is powered by the <u><a href=\"https://www.air.org/project/nces-data-r-project-edsurvey\" style=\"color:blue\">EdSurvey R Package</a></u>. Both InCompass and EdSurvey have similar workflow and design philosophy so users can easily switch between the tools. While users are encouraged to use EdSurvey to get more customized results, InCompass provides a modern user interface to EdSurvey and is well-suited for exploring and conducting quick analyses of ILSA data."),
           HTML(rep("<br>",2)), 
           # title 
           HTML("<p style = \"font-weight: 600; color: #45458f; font-size: 17px;\"> Contact </p>"),
           # text 
           HTML("We welcome your feedback! Please contact us at xxx@air.org with questions, comments, and other inquiries."),
    ), 
    column(4, )
  )
)


# TIMMS Analytic Tab 
mainToolUI = function(nav = "shinyILSA Plots", tab = "shinyILSA Plots") tagList(
  #tags$script('var selectedVars = [];'),
  # guide Text for how to navigate through app 
  tags$div(style = "font-weight: 400; color: black; font-size: 17px;", 
           HTML(paste0(h3("Getting started"), p("To get started, first select your criteria. Then, select variables of interest. Finally, conduct your analysis.")))
  ), 
  HTML(rep("<br>",2)), 
  tags$div(class = "column",
           sidebarLayout(
             tags$div(class = "col-sm-4",
                      tags$form(class="well",
                                HTML("<h2>Step 1: Select Criteria</h2>"),
                                sideBarBasicUI("mainTool")
                                # sideBarResetUI("mainTool"),
                                # sideBarControlPanelUI("mainTool")
                                )
                      
                      # use a sidebar template from global.R and add in other inputs
             ),
             # Apply the plotTableUI function in global.R to the "mainTool" id - this needs to match the id of the module getting called in server.R
             mainPanel(
               plotTableUI("mainTool")
             )
           ))
)

