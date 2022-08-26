# combine new school data into one
data_dir <- "I:/NCES/NCES_Dev/sunjoo_LEE_MOVE/CRDC_outlier_2021_2/Extract 3/"
mods <- list.files(paste0(data_dir, "RU By Module/"))
mod_cols <- list()

# all the school data - first module
sch <- mods[!grepl('LEA', mods)]
df <- read.csv(paste0(data_dir, 'RU By Module/', sch[1])) #"FORMS_Schools.csv"
# all the school data - loop through
for (csv in sch[2:length(sch)]) {
  one <- read.csv(paste0(data_dir,'RU By Module/',csv))
  df <- merge(df, one, by.x = 'COMBOKEY', by.y = 'FormId')
  mod <- gsub("SCH_|.csv", "", csv)
  mod_cols[[mod]] <- tolower(colnames(one))
}

# just the important lea data
lea <- read.csv(paste0(data_dir,'RU By Module/LEA_CHAR.csv'))
lea_ind_vars <- grep('_ind', tolower(colnames(lea)), value = T)
leas <- read.csv(paste0(data_dir, "Forms/LEAForms.csv"))
lea <- merge(lea, leas[, c("Id","State")], by.x = 'FormId', by.y = 'Id', all.x = T)
df <- merge(df, lea, by.x ='LEAID', by.y = 'FormId', all.x = T)

# all negatives to NA
df[df < 0] <- NA
colnames(df) <- tolower(colnames(df))


# now process like old data
the_dir <- "I:/NCES/NCES_Dev/sunjoo_LEE_MOVE/CRDC_outlier_2021_2/"
sourcepath <- paste0(the_dir, "1_data_prep/")
outputPath <- paste0(the_dir, '0_processed_data/crdc_prepped.csv')
source(paste0(sourcepath, 'prepFunctions.R'))

# special (character) vars 
df <- specialVars(df)

# var names that have changed
out_vars <- c("sch_altfocus", "sch_altfocus", "sch_jjtype", "sch_dind_instructiontype", "sch_dind_virtualtype")
keep_vars <- list(
  SCHR=c("sch_altfocus_pre_mean", "sch_altfocus_post_mean"),
  JUST=c("sch_jjtype_pre_mean", "sch_jjtype_post_mean"),
  DIND=c("sch_dind_instructiontype_inperson", "sch_dind_instructiontype_virtual", "sch_dind_instructiontype_hybrid", "sch_dind_instructiontype_ns", "sch_dind_virtualtype_in", "sch_dind_virtualtype_out"),
  ENR=c('tot_enrl','sch_504enr','sch_ideaenr','sch_lepenr','sch_enr_m','sch_enr_f','mf_ratio','enr504_ratio','enridea_ratio','enrlep_ratio'),
  LEA=c("combokey", "sch_name", "leaid", "state", lea_ind_vars)
)


# recollect them here
crdc <- df[ , keep_vars$LEA]
final_mod_cols <- list(LEA=lea_ind_vars)

# combine race and get totals
for (mod in names(mod_cols)) {
  print(mod)
  cols <- mod_cols[[mod]]
  cols <- cols[!cols %in% c("formid", out_vars)]
  cols <- c(cols, keep_vars[[mod]])
  one <- df[ , cols]
  one_df <- combineRace(one)$dat
  # drop tot cols, which are redundant
  schVars <- grep("sch_", colnames(one_df), value = TRUE)
  # total & norm
  if (mod == 'ENRL') {
    one_df <- getTotals(one_df)
    one_df <- normalizeCRDC(one_df)
    schVars <- c(schVars, keep_vars$ENR)
  }
  one_df <- one_df[ , colnames(one_df) %in% schVars]
  crdc <- cbind(crdc, one_df)
  final_mod_cols[[mod]] <- schVars
  
}

# save prepped data
write.csv(crdc, outputPath, row.names = F)

# save module - column info for later
library(rjson)
saveRDS(final_mod_cols, paste0(the_dir,"0_processed_data/mod_col.rds"))
saveRDS(final_mod_cols, paste0(the_dir,"3_shiny_app/data/mod_col.rds"))
cols <- toJSON(final_mod_cols)
fileConn <- file(paste0(the_dir,"0_processed_data/mod_col.txt"))
writeLines(cols, fileConn)
close(fileConn)
