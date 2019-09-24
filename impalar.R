if(!require(easypackages)){install.packages("easypackages")}
library(easypackages)
packages("dplyr", "DBI", "implyr",  prompt = FALSE)

drv <- odbc::odbc()

impala <- implyr::src_impala(drv = drv, dsn = "HDFS PROD")

myDF <- dbGetQuery(
  impala,
  "select contact_contact_name, 
  primary_addr_prov_state 
  from consume_acm.vw_acm_customer_view 
  limit 1000") %>% 
  as_tibble()

dbDisconnect(impala)
rm(drv, impala)

head(myDF)