.libPaths(c(.libPaths(), 'C:\\spark211hadoop27\\R\\lib')) 
Sys.setenv(SPARK_HOME = 'C:\\spark211hadoop27') 
Sys.setenv(PATH = paste(Sys.getenv(c('PATH')), 'C:\\spark211hadoop27\\bin', sep=':')) 
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"))) 

sparkR.session(master = "local[*]", enableHiveSupport = FALSE, 
               sparkConfig = list(spark.driver.memory = "1g", spark.sql.warehouse.dir="c:\\tmp\\")) 

##########################

#Some SparkR sample	code	- Note the dplyr-like functions are realy SparkR functions
#and therefore are created a bit differently. This is the same functionality provided by MapReduce 
# now let's run thorugh a few local examples to confirm that we are working in Spark  
df_spark <- as.DataFrame(faithful)  
dim(df_spark)
printSchema(df_spark)
class(df_spark)  
head(df_spark)  
head(select(df_spark, df_spark$eruptions))  
head(filter(df_spark, df_spark$waiting < 50))  
# Grouping, Aggregation  
head(summarize(groupBy(df_spark, df_spark$waiting), count = n(df_spark$waiting)))  
# Operating on Columns  
df_spark$waiting_secs <- df_spark$waiting * 60   
head(df_spark) 
# stop your session when finished  
sparkR.session.stop()

#More sample code
df_spark_iris <- iris
names(df_spark_iris) <- gsub(names(df_spark_iris), pattern = "\\.", replacement = "_")
df_spark_iris <- as.DataFrame(df_spark_iris)
head(select(df_spark_iris, df_spark_iris$Sepal_Length))
head(filter(df_spark_iris, df_spark_iris$Sepal_Length > 5))
head(summarize(group_by(df_spark_iris, df_spark_iris$Petal_Width), count=n(df_spark_iris$Sepal_Width)))

#GLM Model
## iris
# Fit a linear model over the dataset. 
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df_spark_iris, family = "gaussian")
# Model coefficients are returned in a similar format to R's native glm(). 
summary(model)
# Make predictions based on the model. 
predictions <- predict(model, newData = df_spark_iris) 
head(select(predictions, "Sepal_Length", "prediction"))

##diabetes
install.packages('lars') 
library(lars) 
data(diabetes) 
head(diabetes)
diabetes_all <- data.frame(cbind(diabetes$x, y = diabetes$y)) 
head(diabetes_all) 
outcome_name <- 'y' 
diabetes_all$sex <- as.numeric(as.factor(diabetes_all$sex )) 
dim(diabetes_all)
head(diabetes_all)
# split data into training and testing 
set.seed(1234) 
splitIndex <- base::sample(nrow(diabetes_all), floor(0.75*nrow(diabetes_all))) 
train_diabetes <- diabetes_all[ splitIndex,] 
test_diabetes <- diabetes_all[-splitIndex,]
# Convert local data frame/RDD/etc to a SparkR DataFrame 
train_diabetes_sp <- as.DataFrame(train_diabetes) 
test_diabetes_sp <- as.DataFrame(test_diabetes)
# Fit a linear model over the dataset. 
model <- glm(y~age+sex+bmi+map+tc+ldl+hdl+tch+ltg+glu, data=train_diabetes_sp, family='gaussian') 
summary(model)
# Use test data set to predict y 
predictions <- predict(model, newData = test_diabetes_sp) 
names(predictions)
predictions_details <- select(predictions, predictions$label, predictions$prediction)
# collect errors and calculate the mean squared error (MSE) 
predictions_details <- collect(predictions_details) 
mse <- mean((predictions_details$label - predictions_details$prediction)^2) 
print(mse)#2865.024

##Titanic
#Remeber GLM requires all numeric variables
# Using dataset from the UCI Machine Learning Repository (http://archive.ics.uci.edu/ml/) 
titanicDF <- read.csv('http://math.ucdenver.edu/RTutorial/titanic.txt',sep='\t') 
# Creating new title feature from name field 
titanicDF$Title <- ifelse(grepl('Mr ',titanicDF$Name),'Mr',ifelse(grepl('Mrs',titanicDF$Name),'Mrs',ifelse(grepl('Miss',titanicDF$Name),'Miss','Nothing'))) 
titanicDF$Title <- as.factor(titanicDF$Title) 
# Impute age to remove NAs 
titanicDF$Age[is.na(titanicDF$Age)] <- median(titanicDF$Age, na.rm=T) 
# Reorder data set so target is last column 
titanicDF <- titanicDF[c('PClass', 'Age', 'Sex', 'Title', 'Survived')] 
str(titanicDF)
# dummy text fields 
charcolumns <- names(titanicDF[sapply(titanicDF, is.factor)]) 
charcolumns
for (colname in charcolumns) {
     print(paste(colname,length(unique(titanicDF[, colname]))))
     for (newcol in unique(titanicDF[,colname])){
          if (!is.na(newcol))
               titanicDF[, paste0(colname,"_",newcol)] <- ifelse(titanicDF[, colname] == newcol, 1, 0)
     }
     titanicDF <- titanicDF[,setdiff(names(titanicDF),colname)]
} 
head(titanicDF)
# Split data into training and testing 
set.seed(1234) 
splitIndex <- base::sample(nrow(titanicDF), floor(0.75*nrow(titanicDF))) 
trainDF <- titanicDF[ splitIndex,] 
testDF <- titanicDF[-splitIndex,] 
# Convert local data frame/RDD/etc to a SparkR DataFrame 
#train_titanic_sp <- createDataFrame(sqlContext, trainDF) #Format if connecting to remote Spark cluster
#test_titanic_sp <- createDataFrame(sqlContext, testDF) 
train_titanic_sp <- as.DataFrame(trainDF) 
test_titanic_sp <- as.DataFrame(testDF) 
dim(train_titanic_sp) 
# Fit a linear model over the dataset. 
model <- glm(Survived~., data=train_titanic_sp, family="binomial") 
predictions <- predict(model, newData = test_titanic_sp ) 
names(predictions) 
predictions_details <- select(predictions, predictions$label, predictions$prediction) 
#Model returns probabilities so need to change to 0/1 manually
predictions_details$prediction <- ifelse(predictions_details$prediction < .5, 0, 1)#0.5 might not be the best value
head(predictions_details)
#registerTempTable(predictions_details, "predictions_details") #deprecated
createOrReplaceTempView(predictions_details, "predictions_details")
# Let's calculate the accuracy manually: 
#TP <- sql(sqlContext, "SELECT count(label) FROM predictions_details WHERE label = 1 AND prediction = 1") #Format for remote SPark
TP <- sql("SELECT count(label) FROM predictions_details WHERE label = 1 AND prediction = 1")
TP <- collect(TP)[[1]] 
TN <- sql("SELECT count(label) FROM predictions_details WHERE label = 0 AND prediction = 0") 
TN <- collect(TN)[[1]] 
FP <- sql("SELECT count(label) FROM predictions_details WHERE label = 0 AND prediction = 1") 
FP <- collect(FP)[[1]] 
FN <- sql("SELECT count(label) FROM predictions_details WHERE label = 1 AND prediction = 0") 
FN <- collect(FN)[[1]] 
accuracy = (TP + TN)/(TP + TN + FP + FN) 
print(paste0(round(accuracy * 100,2), '%'))


##### stop your session when finished #####
sparkR.session.stop()


