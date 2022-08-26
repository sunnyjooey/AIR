shinyServer(function(input, output, session) {

  windowWidth <- reactive(input$dimension) # use only width to avoid regenerated graph due to downloading panel pop up

  #parse url to trigger clicking on a tab
  observe({
    data <- parseQueryString(session$clientData$url_search)
    session$sendCustomMessage(type='updateSelections', data)
    #updateSelectizeInput(session, 'subject', list(selected="Math"))
  })
  
  # compareAL
  sideBarBasic("mainTool")
  plotTable("mainTool")
  # About text to variable select tab 
  observeEvent(input$timssApp, {
    data <- list(tab = "timssApp")
    session$sendCustomMessage(type='updateSelections', data)
  })
  # navbar title to About tab 
  observeEvent(input$title, {
    data <- list(tab = "homePage")
    session$sendCustomMessage(type='updateSelections', data)
  })
  # # unsuspendAll()
  # session$onSessionEnded(function() {
  #     stopApp()
  # })

})