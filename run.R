# install.packages("googlesheets")
library('googlesheets')
suppressPackageStartupMessages(library(dplyr))
library(googleAnalyticsR)
library(lubridate)
library(data.table)
library(parallel)
library(doParallel)

numCores <- detectCores()
cl <- makeCluster(numCores - 2) 
registerDoParallel(cl) 


# auth --------------------------------------------------------------------
token <- gs_auth(cache = FALSE)
gd_token()
saveRDS(token, file = "googlesheets_token.rds")
gs_auth(token = "googlesheets_token.rds")


ga_auth()
# google_analytics_account_list()


# read --------------------------------------------------------------------
queries <- gs_title("jose")
gs_read(queries) %>%  View()



# trans -------------------------------------------------------------------
reports <- queries %>% gs_read(ws = 1, range = "A2:Q15")
reports <- rbind(colnames(reports), reports)
reports <- tbl_df(t(reports) )
colnames(reports) <- reports[1,]
reports <- reports[-1, ]
row.names(reports) <- reports$`Report Name`
reports$`Report Name` <- NULL
reports %>%  View()


# trans2 ------------------------------------------------------------------


tmp <-  tempfile()
fwrite(reports, tmp, row.names = T)
reports <- fread(tmp, check.names = T)
colnames(reports)[1] <- "Report"
reports$Report
reports.list <- split(reports, seq(nrow(reports)))
reports.list <- setNames(split(reports, seq(nrow(reports))), reports$Report)
# colnames(reports.list$Q1)




# ga data -----------------------------------------------------------------

month_limit <- 1
date_range <- c(as.character(floor_date(Sys.Date() - months(month_limit), "month")) , as.character(Sys.Date()-1))

getGaData4 <- function(query, date_range) {
  require(data.table)
  googleAuthR::gar_auth()
  print(query$View..Profile..ID...ids)
  ga_id <- query$View..Profile..ID...ids

  ga <- google_analytics_4(
    viewId = query$View..Profile..ID...ids,
    date_range = as.character(date_range),
    metrics = as.vector(unlist(strsplit(query$Metrics, ","), use.names = F)),
    dimensions = as.vector(unlist(strsplit(query$Dimensions, ","), use.names = F)),
    anti_sample = TRUE
  )
  DT <- as.data.table(ga)
  # DT[, report:= query$Report]
  return(DT)
}

# foo <- getGaData4(reports.list$Q1, date_range)

ga_data <- foreach(i=1:length(reports.list), .packages = c("googleAnalyticsR","googleAuthR","lubridate", "data.table"), .final = function(x) setNames(x, names(reports.list)), .combine='list', .multicombine=TRUE, .inorder = T) %dopar% getGaData4(reports.list[[i]], date_range)

ga_data$Q1
# fwrite(ga_data$Q1, "klipflio.csv")


# 2csv --------------------------------------------------------------------
# ga_data[[1]][, c(1:(ncol(ga_data[[1]])-1)), with=F]

foreach(i=1:length(ga_data), .packages = c("googlesheets"), .inorder = T) %dopar%  fwrite(ga_data[[i]], paste0("csv/",names(ga_data[i]),".csv"))


# save to gs --------------------------------------------------------------

save_to_sheet <- function(queries, title, df) {
  suppressMessages(gs_auth(token = "googlesheets_token.rds", verbose = FALSE))
  gs_ws_new(queries, ws_title = title, input=df)
  
}
queries <- gs_title("jose")
foreach(i=1:length(ga_data), .packages = c("googlesheets"), .inorder = T) %dopar% save_to_sheet(queries, names(ga_data[i]),ga_data[[i]])



# klipfolio ---------------------------------------------------------------



kf.list <- ga_data
names(kf.list) <- c('dc80893f3ea03c24353a825243ddd846','2c37e68ac2fa9641a6f514682f617ccf')
# GET https://app.klipfolio.com/api/1/datasources/45947df1d78e8b246335238cdcf70b96


# upload_klipolio  <- function(csv, data_source) {
#   system('curl https://app.klipfolio.com/api/1/datasource-instances/45947df1d78e8b246335238cdcf70b96/data -X PUT --upload-file  klipflio.csv --header "kf-api-key:c48f1305307152134ef2bd2ade525d9a927f2985" -H Content-Type:application/csv', wait = T )
# }

foreach(i=1:length(kf.list), .inorder = F) %dopar% system(paste0('curl https://app.klipfolio.com/api/1/datasource-instances/',names(kf.list[i]),'/data -X PUT --upload-file  ',file.path('csv',paste0(names(ga_data[i]),'.csv')),' --header "kf-api-key:c48f1305307152134ef2bd2ade525d9a927f2985" -H Content-Type:application/csv'), wait = T, ignore.stdout = F )






###
system("curl https://app.klipfolio.com/api/1/datasources -d \"{'name': 'Example','description': 'This is a new data source', 'format': 'csv', 'connector': 'box', 'refresh_interval': 0, 'properties': {'endpoint_url': 'http://test/data/scatter.xml', 'method': 'GET'}}\" --header 'kf-api-key:c48f1305307152134ef2bd2ade525d9a927f2985' -H Content-Type:application/json " )



