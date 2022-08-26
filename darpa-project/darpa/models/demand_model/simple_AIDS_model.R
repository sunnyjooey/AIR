
DEFAULT_REPO_URL = "https://cloud.r-project.org"

install.packages("micEconAids", repos=DEFAULT_REPO_URL)
install.packages("hash", repos=DEFAULT_REPO_URL)
install.packages("readxl", repos=DEFAULT_REPO_URL)
install.packages("configr", repos=DEFAULT_REPO_URL)

#Estimation of AIDS model

library(micEconAids)
library(hash)
library(readxl)
library(configr)

config <- read.config(file = Sys.getenv("R_CONFIGFILE_ACTIVE", "luigi.cfg"))
filepath <- file.path(config$paths$output_path)

waves_all <- c(1,2,3,4)
state <- c('Central Equatoria', 'Eastern Equatoria', 'Lakes', 'Northern Bahr el Ghazal',
           'Warrap', 'Western Bahr el Ghazal', 'Western Equatoria')

train_data <- read.csv(file.path(filepath, 'aids_model_train.csv'), header=TRUE, sep=",")
test_data <- read.csv(file.path(filepath, 'aids_model_test.csv'), header=TRUE, sep=",")
exog_data <- read.csv(file.path(filepath, 'demand_model_inputs_clean.csv'), header=TRUE, sep=",")

cols <- colnames(train_data)
pcols <- cols[grep("^[p].*", cols)]
wcols <- cols[grep("^[w].*", cols)][cols[grep("^[w].*", cols)]!="wave"]
commodity <- substr(wcols,2,200) #List of commodities predicted
qcols <- paste("q",substr(wcols,2,200),sep="")
priceNames <- pcols
shareNames <- wcols

#declaring output matrices
training_output <- matrix(nrow=0,ncol=length(c('hhid','adult_equivalent','state',wcols,qcols)))
test_output <- matrix(nrow=0,ncol=length(c('hhid','adult_equivalent','state',wcols,qcols)))

colnames(training_output) = c('hhid','adult_equivalent','state',wcols,qcols)
colnames(test_output) = c('hhid','adult_equivalent','state',wcols,qcols)

# ---------------
# Train the Model
# ---------------
#AIDS formulation for training model			
aidsResult <- aidsEst( priceNames, shareNames, "xFood", data = train_data, method = "IL" , priceIndex='P') #Iterative linear least squares estimation

#Calculate quantities based on weight, expenditure and price
train_data[qcols] = train_data[wcols]*train_data$xFood/train_data[pcols]

#Estimate demand for training data using model and save output
AIDS_pred_train <- aidsCalc( priceNames, "xFood", coef( aidsResult ), data = train_data )
AIDS_pred_train <- cbind(train_data[c('hhid','adult_equivalent','state','wave','xFood')],AIDS_pred_train$shares,AIDS_pred_train$quant)
colnames(AIDS_pred_train)=c('hhid','adult_equivalent','state','wave','xFood',wcols,qcols)
training_output <- rbind(training_output,AIDS_pred_train)

# --------------
# Test the Model
# --------------
# AIDS prediction for test data using trained model
AIDS_pred_test <- aidsCalc( priceNames, "xFood", coef( aidsResult ), data = test_data, priceIndex='TL' )

#Estimate of per-HH consumption by commodity group for test data 
test_data[qcols] = test_data[wcols]*test_data$xFood/test_data[pcols]

# -------------------------------
# Run the Model on Exogenous Data
# -------------------------------
AIDS_exog_results <- aidsCalc( priceNames, "xFood", coef( aidsResult ), data = exog_data, priceIndex='TL' )

#Save output for test
AIDS_exog_results <- cbind(AIDS_exog_results$shares,AIDS_exog_results$quant,exog_data$X)
colnames(AIDS_exog_results)=c(wcols,qcols,'X')
write.csv(AIDS_exog_results, file=file.path(filepath, 'AIDS_results.csv'))

