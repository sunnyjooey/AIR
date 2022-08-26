plotCitation <- "\n"
opar <- par() # original par
ggrepelSeed <- 1234

# mypalette <- darken(brewer.pal(7,"Set1"))

linePlot <- function(data, title = NULL, windowWidth, sample = sample, statistic = statistic, input) {
  
  # g <- ggplotly(
  g <- 
    ggplot(data, aes(x=`Week since initial mailing`, y=`Percentage`,
                         text =  paste0('Data collection method: ', `Data collection`,
                                        '</br>', '</br>Weeks since initial mailing: ', 
                                        `Week since initial mailing`, 
                                        '</br>', 'Response rate: ', 
                                        percent(`Percentage`/100)) )) +
      geom_line(aes(group = `Data collection`, color = `Data collection`), alpha = 0.8) + 
      labs(x = "Weeks since initial mailing", y = "Response Rate", color = "", alpha = "", title= data$FIGURENAME[1]) +
      scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, by = 10), 
                         labels = c("0%" , "10%", "20%", "30%", "40%", "50%",
                                    "60%", "70%", "80%", "90%", "100%")) +
      theme_hc() + 
      theme(axis.text.x = element_text(angle = 45), 
            text=element_text(family = "Calibri"), 
            axis.ticks = element_blank()) +
      geom_vline(xintercept = 22.5,  color = "black", size= 0.3) + 
      annotate("text", x = 28, y = 85, label = "End of 2017 data collection")
    #   ,
    # tooltip = "text")
    g
}



barPlot <- function(data, title = NULL, windowWidth, sample = sample, statistic = statistic, input) {
   dput(input)
  
  # p <- ggplotly(
  p <- 
    ggplot(data, aes(x = reorder(Level, desc(`Percentage`)), y = Percentage,
                     text = paste0("Response Rate: ", round(`Percentage`, 2), "%"))) + 
    geom_col(width = 0.5, color = "steelblue", fill = "steelblue") +
    labs(x = "Screener status", title= data$FIGURENAME[1]) +
    scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, by = 10), 
                       labels = c("0%" , "10%", "20%", "30%", "40%", "50%",
                                  "60%", "70%", "80%", "90%", "100%")) +
    theme_hc() + theme(axis.ticks = element_blank())
    # , 
    # tooltip = "text")
    p
}



groupedBarPlot <- function(data, title = NULL, windowWidth, sample = sample, statistic = statistic, input) {
  # p <- ggplotly(
  p <- 
    ggplot(data, aes(x = Level, y = Percentage, fill = Grouping,
                            text = paste0("Mode: ", Grouping,
                                          "</br></br>Response Rate: ", round(`Percentage`, 2), "%") )) + 
    geom_col(position = "dodge", width = 0.6) +
    labs(x = "", y = "", fill = "" , title= data$FIGURENAME[1]) +
    theme_hc() + theme(legend.position = "bottom") +
    scale_y_continuous(limits = c(0, 100), breaks = seq(0, 100, by = 10), 
                       labels = c("0%" , "10%", "20%", "30%", "40%", "50%",
                                  "60%", "70%", "80%", "90%", "100%")) +
    scale_fill_manual("Grouping", values = mypalette)
    # , 
    # tooltip = "text")  %>%
    # layout(legend = list(orientation = "h", x = 0.3, y = -0.2))
    p
}



stackedBarPlot <- function(data, title = NULL, windowWidth, sample = sample, statistic = statistic,input) {



  # g <- ggplotly(
  g <- 
    ggplot(data, aes(x = Include, y = Percentage, fill = Level,
                         text = paste0("Count: ", 
                                       "</br></br>Percentage: ", round(Percentage, 2), "%") )) + 
                  geom_col(position = "fill", width = 0.4) +
                  labs(x = "Percentage", y = "", fill = "", title= data$FIGURENAME[1]) +
                  theme_hc() + coord_flip() +
                  scale_fill_manual("Level", values = mypalette)
  
                  # scale_x_continuous(limits = c(0, 100), breaks = seq(0, 100, by = 10), 
                  #                    labels = c("0%" , "10%", "20%", "30%", "40%", "50%",
                  #                               "60%", "70%", "80%", "90%", "100%"))
                #   , 
                # tooltip = "text")
  g
}
