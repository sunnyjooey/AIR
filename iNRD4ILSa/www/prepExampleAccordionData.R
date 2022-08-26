source("helperFunctions.R",local = TRUE)

# load variable spreadsheet, prep for the filtered codebook, create pre-processed accordion objects to save time
all_files <- list.files("data/")
the_file <- grep("T11_15_G4_G8_codebook", all_files, value = T) 
combinedCodebookFile <- read.csv(paste0("data/",the_file))

# pre-process the combinedCodebookFile object (because later the bsplus functions can't deal with "<" or ">" properly)
combinedCodebookFile$Labels <- gsub("<",'[', combinedCodebookFile$Labels)
combinedCodebookFile$Labels <- gsub(">",']', combinedCodebookFile$Labels)
combinedCodebookFile$labelValues <- gsub("<",'[', combinedCodebookFile$labelValues)
combinedCodebookFile$labelValues <- gsub(">",']', combinedCodebookFile$labelValues)
combinedCodebookFile$des<- gsub("<",'[', combinedCodebookFile$des)
combinedCodebookFile$des <- gsub(">",']', combinedCodebookFile$des)

#g4 2011 & 2015
preprocessedAccordion_G4_2011_2015 <- combinedCodebookFile %>% 
  filter(grade %in% "Grade 4") %>% 
  create_accordion(ns = NS("mainTool"))
saveRDS(preprocessedAccordion_G4_2011_2015, paste0(getwd(),"/data/", "preprocessedAccordion_G4_2011_2015", ".rds"))

#g4 2011
preprocessedAccordion_G4_2011 <- combinedCodebookFile %>% 
  filter(grade %in% "Grade 4") %>% 
  filter(!is.na(variableName2011)) %>% 
  create_accordion(ns = NS("mainTool"))
saveRDS(preprocessedAccordion_G4_2011, paste0(getwd(),"/data/", "preprocessedAccordion_G4_2011", ".rds"))

#g4 2015
preprocessedAccordion_G4_2015 <- combinedCodebookFile %>% 
  filter(grade %in% "Grade 4") %>% 
  filter(!is.na(variableName2015)) %>% 
  create_accordion(ns = NS("mainTool"))
saveRDS(preprocessedAccordion_G4_2015, paste0(getwd(),"/data/", "preprocessedAccordion_G4_2015", ".rds"))

#g8 2011 & 2015
preprocessedAccordion_G8_2011_2015 <- combinedCodebookFile %>% 
  filter(grade %in% "Grade 8") %>% 
  create_accordion(ns = NS("mainTool"))
saveRDS(preprocessedAccordion_G8_2011_2015, paste0(getwd(),"/data/", "preprocessedAccordion_G8_2011_2015", ".rds"))

#g8 2011
preprocessedAccordion_G8_2011 <- combinedCodebookFile %>% 
  filter(grade %in% "Grade 8") %>% 
  filter(!is.na(variableName2011)) %>% 
  create_accordion(ns = NS("mainTool"))
saveRDS(preprocessedAccordion_G8_2011, paste0(getwd(),"/data/", "preprocessedAccordion_G8_2011", ".rds"))

#g8 2015
preprocessedAccordion_G8_2015 <- combinedCodebookFile %>% 
  filter(grade %in% "Grade 8") %>% 
  filter(!is.na(variableName2015)) %>% 
  create_accordion(ns = NS("mainTool"))
saveRDS(preprocessedAccordion_G8_2015, paste0(getwd(),"/data/", "preprocessedAccordion_G8_2015", ".rds"))

