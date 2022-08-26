thedir <- 'I:/NCES/NCES_Dev/sunjoo_LEE_MOVE/CRDC_outlier_2021_2/' 
NUM_NEIGHBORS <- 20 #change this!

### combine data code
ocr_regions <- c("Atlanta", "Seattle", "Denver", "Kansas City", "San Fransisco", "Boston", "Philadelphia", "Chicago", "Dallas", "Cleveland", "New York", "DC")
mod <- readRDS("I:/NCES/NCES_Dev/sunjoo_LEE_MOVE/CRDC_outlier_2021_2/0_processed_data/mod_col.rds")

# # read through directories
loc <- paste0(thedir, '/2_modeling/results/')
# los <- list.files(loc)

# combine raw and modeling and zscore data 
df <- read.csv(paste0(thedir, '/0_processed_data/crdc_prepped.csv'))
data <- read.csv(paste0(thedir, '/0_processed_data/crdc_prepped_formod.csv'))
thecores <- read.csv(paste0(thedir, '/2_modeling/results/df_scores_CBLOF_50035.csv'))
data <- cbind(df[ , c('sch_name','state', 'leaid')], data)
data <- cbind(data, thecores)

# overall outlier score
lo <- read.csv(paste0(thedir, '/2_modeling/results/df_scores.csv'))
data$out_score <- lo$score
rm(df)
rm(thecores)

# get ocr region
geo <- read.csv(paste0(thedir, '/0_processed_data/ocr_region.csv'))
geo$state <- tolower(geo$state)
data <- merge(data,geo, by='state', all.x=T)
data <- data[!is.na(data$region), ] #why school no state?

#### data files to save
all_dfs <- list()
all_diffs <- list()
all_neigh <- list()

for (oreg in ocr_regions) {
  print(oreg)
  # module data
  data_reg <- data[data$region==oreg, ]
  diff_cols <- grep("NO.", colnames(data_reg), value = T)
  diff_score <- data_reg[ , diff_cols]
  # change to z scores
  z <- apply(as.matrix(diff_score), 1, scale)
  z <- t(z)
  data_reg[ , diff_cols] <- z
  colnames(data_reg)[which(colnames(data_reg) %in% diff_cols)] <- names(mod)
  data_reg$region <- NULL 
  
  # neighbors
  neigh <- read.csv(paste0(loc, 'neigh_', oreg, '.csv')) 
  data_reg <- cbind(data_reg, neigh)
  
  # reorder by outlier score
  data_reg <- data_reg[order(-data_reg$out_score), ]   
  
  # save
  neigh_cols <- paste0('X', 0:(NUM_NEIGHBORS-1))
  data_cols <- colnames(data_reg)[!colnames(data_reg) %in% c(neigh_cols, names(mod), 'out_score')]
  all_dfs[[oreg]] <- data_reg[ , data_cols]
  all_diffs[[oreg]] <- data_reg[ , names(mod)]
  all_neigh[[oreg]] <- data_reg[ , neigh_cols]
}

saveRDS(all_dfs, paste0(thedir, '/3_shiny_app/data/all_dfs.rds'))
saveRDS(all_diffs, paste0(thedir, '/3_shiny_app/data/all_diffs.rds'))
saveRDS(all_neigh, paste0(thedir, '/3_shiny_app/data/all_neigh.rds'))

