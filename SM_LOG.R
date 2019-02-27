#-----------------------------------------------------------------------------------------------------------------------------------------------------------------
clc <- function() {
  while (sink.number() > 0) sink(file=NULL)
  rm(list=ls()); gc()
  gc()
  cat("\014")
  cat("All files discriptors closed.\n")
  cat("Whole memory cleaned.\n")
}

clc()

#-----------------------TOMCAT Access Log---------------------
readLogTomcatAccess <- function(setwd, tomcat) {
  
  cat("\f")
  #setwd = setwd("/home/rstudio/WORK/LOGANALIZE/web/tomcat\ 8_5")
  
  tomcat <- do.call("rbind.fill", 
                    lapply(list.files(
                      #path = "/home/rstudio/WORK/LOGANALIZE/web/tomcat\ 8_5",
                      path = setwd,
                      pattern = "localhost_access_log*"), 
                      
                      function(x) read_delim(x, delim = " ", quote = "^M", col_names = F , skip = 0, skip_empty_rows = T) ))
  
  
  
  #tomcat <- transform(tomcat, X7 = colsplit(X7, pattern  = "&", names = 1:ncol(tomcat)))
  tomcat <- transform(tomcat, X7 = colsplit(X7, pattern  = "&", names(tomcat)))
  tomcat$date <- paste(tomcat$X4, tomcat$X5, sep = " ")
  #tomcat$X7 <- gsub("^[/]", '', tomcat$X7)
  tomcat$user <- tomcat$X3
  tomcat$front <- unlist(tomcat$X7[2])
  tomcat$dc <- unlist(tomcat$X7[3])
  tomcat$code <- tomcat$X9
  
  tomcat <- subset(tomcat, select = -grep("^X", names(tomcat)))
  tomcat <- tomcat[-which(tomcat$dc == ""), ]
  tomcat <- tomcat[-which(tomcat$user == "-"), ]
  
  tomcat <- tomcat %>% tidyr::drop_na()
  rownames(tomcat) <- NULL
  
  return(tomcat)
}

TOMCAT <- 0 
#readLogTomcatAccess(setwd = setwd("/home/rstudio/WORK/LOGANALIZE/web/tomcat\ 8_5"), tomcat = tomcat)
TOMCAT <- readLogTomcatAccess(setwd = setwd("/home/rstudio/WORK/LOGANALIZE/web/tomcat\ 8_5"), tomcat = tomcat)
TOMCAT$tomcat <- "8_5"
setwd = setwd("/home/rstudio/WORK/LOGANALIZE/web/tomcat\ 8_5_2/")
TOMCAT2 <- readLogTomcatAccess(setwd = setwd("/home/rstudio/WORK/LOGANALIZE/web/tomcat\ 8_5_2/"), tomcat = tomcat)  
TOMCAT2$tomcat <- "8_5_2"

#-----------------------APACHE Access Log---------------------
readLogApacheAccess <- function(setwd, apache) {
  
  cat("\f")
  setwd("/home/rstudio/WORK/LOGANALIZE/web/apache")
  
  apache <- do.call("rbind.fill", 
                    lapply(list.files(
                      path = "/home/rstudio/WORK/LOGANALIZE/web/apache",
                      pattern = "access.*"), 
                      
                      function(x) read_delim(x, delim = "\t", quote = "^M", col_names = F , skip = 0, skip_empty_rows = T) ))
  
  
  
  apache <- transform(apache, X9 = colsplit(X9, pattern  = "&", names = 1:ncol(apache)))
  apache$date <- apache$X1
  apache$ip <- apache$X5
  apache$hostname <- apache$X4
  apache$front <- unlist(apache$X9[2])
  apache$dc <- unlist(apache$X9[3])
  apache <- subset(apache, select = -grep("^X", names(apache)))
  apache <- apache[-which(apache$dc == ""), ]
  apache$dc <- gsub(" HTTP/1.1", "", apache$dc)
  
  apache <- apache[!duplicated(apache),]
  apache <- apache %>% tidyr::drop_na()
  rownames(apache) <- NULL
  
  return(apache)
}

APACHE <- readLogApacheAccess(setwd, apache = apache)  

#--------------------------------------------AGG WEB-----------------------------------------------------------------------------------------------------------------
TOMCAT_AGG <- rbind(TOMCAT, TOMCAT2)

WEB <- inner_join(APACHE, TOMCAT_AGG,by = c("date","front","dc"))
WEB <- WEB[!duplicated(WEB),]
WEB <- WEB %>% filter(grepl("^_dc=", WEB$dc))
WEB$user <-unlist(lapply(sapply(WEB$user, function(x) strsplit(x, "\\", fixed = T)), '[[',2))
WEB <- WEB[,c(1,3,6,8)]

APACHE <- NULL
TOMCAT <- NULL
TOMCAT_AGG <- NULL
TOMCAT2 <- NULL

WEB$date <- gsub("\\[", "", gsub("\\+0300]", "", WEB$date))
WEB$start_time <- substr(WEB$date, 13,20)
WEB$date <- substr(WEB$date, 1,11)
WEB$date <- as.Date(lubridate::parse_date_time(WEB$date, orders = c("dmy", "mdy", "ymd")))
#----------------------------------------------------LOG-------------------------------------------------------------------------------------------------------------
readLogSM <- function(log) {
  
  cat("\f")
  lapply(c("purrr","httr", "tm","plyr","Rcrawler", "reshape2", "dplyr", 
           "ggplot2", "stringr", "rvest", "curl", "RCurl", "shiny", 
           "tidyr","shinydashboard","highcharter"), 
         require, character.only = T)
  
  setwd("/home/rstudio/WORK/LOGANALIZE/sm/131")
  
  log <- do.call("rbind.fill", 
                lapply(list.files(
                  path = "/home/rstudio/WORK/LOGANALIZE/sm/131",
                  pattern = "log_*"), 
                  
                  function(x) read_delim(x, delim = " ", quote = "^M", col_names = F , skip = 0, skip_empty_rows = T) ))
                  
                  # function(x) read.csv2(x,
                  #                       sep = "(",
                  #                       header = F,
                  #                       quote = '')))
  
  log$PID <- as.numeric(gsub("\\(", "", log$X1))
  log$TID <- as.numeric(gsub("\\)", "", log$X2))
  
  log$date <- as.Date(lubridate::parse_date_time(log$X3, orders = c("dmy", "mdy", "ymd")))
  log$time <- ts(sapply(str_split(lubridate::parse_date_time(log$X4, "HMS"), " "), "[", 2))
  
  log$component <- log$X5
  log$loglevel <- log$X6
  
  #cols <- names(log[,c(7:22)])
  log$message <- apply(log[, names(log[,c(7:22)])], 1, paste, collapse = " ")
  #drop <- grep("^X", names(log))
  log <- subset(log, select = -grep("^X", names(log)))
  log$message <- gsub("NA", "", log$message)
  log <- log %>% tidyr::drop_na()
  
  # logLevel
  # F (Fatal) 
  # E (Error) 
  # W (Warning) 
  # I (Information) 
  # D (Debug) 
  # A (Alert) 
  #log[,c("component", "loglevel")] <- gsub("^\\s", "", log[,c("component", "loglevel")])
  log$loglevel <- gsub("^\\s+", "", log$loglevel)
  log$component <- gsub("^\\s+", "", log$component)
  level <- c("F","E","W","I","D","A")
  log$loglevel <- ifelse(log$loglevel %in% level, log$loglevel, "I")
  log$message <- ifelse(log$loglevel == "E", paste("[ERROR]", log$message, sep = "':" ), log$message)
  
  log$component <- gsub("JRTE", "RTE", log$component)
  
  rownames(log) <- NULL
  
#   log <- transform(log, V2 = colsplit(V2, pattern  = " ", names = 1:ncol(log)))
#   log$date <- as.Date(lubridate::parse_date_time(log$date, orders = c("dmy", "mdy", "ymd")))
#   log$time <- ts(sapply(str_split(lubridate::parse_date_time(log$time, "HMS"), " "), "[", 2))
#   
#   #write_csv(LOG, "/home/rstudio/WORK/LOGANALIZE/LOG_SM.csv")
  return(log)
  }

LOG <- readLogSM(log = log)

#------------MAKE ID  for Filter Unconditional Records-------------------------------------------------------------------------------------------------
LOG$lin <-  sapply(LOG$message, function(x) ifelse(grepl("has logged in", x), x, x[] <-0))
LOG$lot <-  sapply(LOG$message, function(x) ifelse(grepl("logged out", x), x, x[] <-0))
LOG$lin <- ifelse(LOG$lin !=0, LOG$lin <- LOG$time, 0)
LOG$lot <- ifelse(LOG$lot !=0, LOG$lot <- LOG$time, 0)
#------------------------------------------------------------------------------------------------------------------------------------------------------
user <- LOG %>% 
  filter(grepl("Process|Host|^SOAP|GUID|ERROR|User", message)) %>% 
  dplyr::select(-c(component, loglevel)) %>% 
  group_by(PID, TID,date) %>% 
  mutate_all(funs(start = min(lin), end = max(lot))) %>% 
  dplyr::select(PID,TID,date, message,time,  lin, lot) %>% 
  arrange(date) 

user$name <- ifelse(grepl("^User", user$message), sapply(strsplit(user$message, " "), "[", 2), 0)
user <- as.data.frame(apply(user, 2, function(x)  trimws(x, which = c("both"))), stringsAsFactors = F)
user$name <- gsub("_", "", user$name)
#-------------------------------
tmp <- user[,c(1:3,5:8)]
tmp_1 <- tmp[tmp$lin >0,]
tmp_2 <- tmp[tmp$lot >0,]
tmp<-NULL

total <- inner_join(tmp_1,tmp_2, by = c("PID" = "PID", "TID" = "TID", "date" = "date", "name" = "name"))
tmp_1 <- NULL
tmp_2 <- NULL
# tt <- total
# total <- NULL
# t<-as.data.frame(cbind(unique(tt$time.x),unique(tt$time.y)), stringsAsFactors = F)
# tt <- tt[,c(1,4)]
# t_2 <- inner_join(t, tt, by = c("V1"="time.x"))
# t_2 <- data.frame(t_2[!duplicated(t_2),])

grand_total <- inner_join(user, total, by = c("PID" = "PID", "TID" = "TID", "date" = "date", "name" = "name", "time" = "time.x"))
grand_total <- grand_total[grand_total$lin >0,]
grand_total <- grand_total[,c(1,2,3,8,9,13)]
grand_total <- inner_join(user, grand_total, by = c("PID" = "PID", "TID" = "TID", "date" = "date", "time" = "lin.x"))
grand_total <- grand_total[,c(1:5,9,10)]
grand_total <- grand_total %>% 
  filter(grepl("Process|Host|^SOAP|GUID|ERROR", message))
colnames(grand_total) <- c("PID","TID","date","message","start_time","Username","end_time")


#grand_total$Browser <- ifelse(grepl("^SOAP", grand_total$message), sapply(strsplit(grand_total$message, "Browser"), "[", 2), 0)
grand_total <- unite(grand_total, "agg", c("PID","TID","date","start_time","Username","end_time"))
grand_total <- data.frame(grand_total[!duplicated(grand_total),])

grand_total <- aggregate(message~agg, data = grand_total, paste0, collapse="_")


grand_total <- transform(grand_total, message = colsplit(message, pattern  = "_", names = c("Process","Host","SOAP","GUID")))


FINAL <- transform(grand_total, message = colsplit(agg, pattern  = "_", 
                                                         names = c("PID","TID","date","start_time","Username","end_time")))
FINAL <- cbind(FINAL, grand_total)


FINAL <- FINAL[,c(2,4)]
FINAL$PID <- unlist(FINAL$message$PID)
FINAL$TID <- unlist(FINAL$message$TID)
FINAL$date <- unlist(FINAL$message$date)
FINAL$start_time <- unlist(FINAL$message$start_time)
FINAL$end_time <- unlist(FINAL$message$end_time)
FINAL$Username <- unlist(FINAL$message$Username)
FINAL$Process <- unlist(FINAL$message.1$Process)
FINAL$Host <- unlist(FINAL$message.1$Host)
FINAL$SOAP <- unlist(FINAL$message.1$SOAP)
FINAL$GUID <- unlist(FINAL$message.1$GUID)
FINAL <- FINAL[,-c(1:2)]
FINAL$Username <- gsub("^mr", "mr_", FINAL$Username)

# FINAL$GUID <-  apply(FINAL, 2, function(x) ifelse(grepl("GUID=", x), x, x[] <-0))
# FINAL$GUID <- gsub("GUID=", "", FINAL$GUID)

#--------Extract Process-----------
#process <- as.data.frame(apply(user[grepl("V", names(user)),], 2, function(x) ifelse(grepl("Process", x), x, x[] <-0)))
process <- as.data.frame(apply(FINAL, 2, function(x) ifelse(grepl("Process", x), x, x[] <-0)))  
process <- process %>%
  unite(PROCESS, colnames(process)) %>% 
  mutate_all(funs(gsub("_0", "", .))) %>% 
  mutate_all(funs(gsub("0_", "", .)))
process$PROCESS <- as.numeric(ifelse(process$PROCESS !=0, unlist(lapply(strsplit(process$PROCESS, ' '), function(x) x[6])), 0))
#--------Extract GUID-----------
#guid <- as.data.frame(apply(user[grepl("V", names(user)),], 2, function(x) ifelse(grepl("GUID", x), x, x[] <-0)))
guid <- as.data.frame(apply(FINAL, 2, function(x) ifelse(grepl("GUID", x), x, x[] <-0)))  
guid <- guid %>%
  unite(GUID, colnames(guid)) %>% 
  mutate_all(funs(gsub("0_", "", .))) %>% 
  mutate_all(funs(gsub("_0", "", .))) %>% 
  mutate_all(funs(gsub("GUID=", "", .))) 
#--------Extract HOST-----------
host <- as.data.frame(apply(FINAL, 2, function(x) ifelse(grepl("Host", x), x, x[] <-0)))  
host <- host %>%
  unite(HOST, colnames(host)) %>% 
  mutate_all(funs(gsub("0_", "", .))) %>% 
  mutate_all(funs(gsub("_0", "", .))) %>% 
  mutate_all(funs(gsub("Host network address:", "", .))) 
#--------Extract SOAP-----------
soap <- as.data.frame(apply(FINAL, 2, function(x) ifelse(grepl("SOAP", x), x, x[] <-0)))  
soap <- soap %>%
  unite(SOAP, colnames(soap)) %>% 
  mutate_all(funs(gsub("0_", "", .))) %>% 
  mutate_all(funs(gsub("_0", "", .)))
#-------------------------------------------  
FINAL <- FINAL[,c(1:6)]
FINAL <- cbind(FINAL, process, host, soap, guid)

guid <- NULL
host <- NULL
soap <- NULL
process <- NULL
FINAL$duration <- sapply(as.duration(hms(FINAL$end_time) - hms(FINAL$start_time)), "[")

#-----------------------------------------------------------------------------------------------------------------------------------------------------
# user <- user %>% 
#   #group_by(agg, message) %>% 
#   #filter(grepl("Process|Host|^SOAP|SSL|Set trusted|GUID|ERROR", message)) %>%
#   filter(grepl("Process|Host", message)) %>% 
#   dplyr::mutate(rn = paste0("V", 1:n())) %>%
#   tidyr::spread(rn, message)
#----Extract Process
#------------------------SYSLOG------------------------------------------------------------------------------------------------------------------------
readSLog <- function(slog) {
  
  cat("\f")
  setwd("/home/rstudio/WORK/LOGANALIZE/")
  slog <- do.call("rbind.fill", 
                 lapply(list.files(path = "/home/rstudio/WORK/LOGANALIZE/",
                                   pattern = "syslog.csv"), 
                        function(x) read_delim(x, delim = ",", quote = " ", col_names = T , skip = 0, skip_empty_rows = T) ))
  slog <- slog %>% tidyr::drop_na()
  slog <- slog[,c(3,4,5,8,9,13)]
  colnames(slog) <- gsub('(\\\")', '', colnames(slog))
  slog <- as.data.frame(apply(slog, 2, function(x) gsub('(\\")', '', x)), stringsAsFactors = F)
  slog$date <- sapply(strsplit(as.character(slog$START_TIME), " "), "[", 1)
  slog$START_TIME <- sapply(strsplit(as.character(slog$START_TIME), " "), "[", 2)
  slog$STOP_TIME <- sapply(strsplit(as.character(slog$STOP_TIME), " "), "[", 2 )
  slog$date <-as.Date(lubridate::parse_date_time(slog$date, orders = c("dmy", "mdy", "ymd")))
  # slog$START_TIME <- sapply(strsplit(as.character(as.POSIXlt(slog$START_TIME, format = '%H:%M:%S') + 3 * 60 * 60), " "), "[",2)
  # slog$END_TIME <- sapply(strsplit(as.character(as.POSIXlt(slog$END_TIME, format = '%H:%M:%S') + 3 * 60 * 60), " "), "[",2)
  
  
  rownames(slog) <- NULL
  return(slog)
}

SLOG <- readSLog(slog = slog)

SLOG$START_TIME <- sapply(strsplit(as.character(as.POSIXlt(SLOG$START_TIME, format = '%H:%M:%S') + 3 * 60 * 60), " "), "[",2)
SLOG$STOP_TIME <- sapply(strsplit(as.character(as.POSIXlt(SLOG$STOP_TIME, format = '%H:%M:%S') + 3 * 60 * 60), " "), "[",2)
#------------------------MSGLOG------------------------------------------------------------------------------------------------------
readMSGLogSM <- function(msglog) {
  cat("\f")
  setwd("/home/rstudio/WORK/LOGANALIZE/")
  msglog <- do.call("rbind.fill", 
                  lapply(list.files(path = "/home/rstudio/WORK/LOGANALIZE/",
                                    pattern = "msglog.csv"), 
                         function(x) read.csv(x, sep = ",", header = T, fileEncoding = "cp1251")))
                         #function(x) read_delim(x, delim = ",", quote = "^M", col_names = T , skip = 0, skip_empty_rows = F ) ))
  colnames(msglog) <- gsub('"', "", colnames(msglog))
  msglog <- as.data.frame(apply(msglog, 2, function(x) gsub('"', "", x)), stringsAsFactors = F)
  msglog <- msglog %>% tidyr::drop_na()
  rownames(msglog) <- NULL
  return(msglog)
}
MSGLOG <- readMSGLogSM(msglog = msglog)
#------------------------------------------------------------------------------------------------------------------------------------


#W_SM <- inner_join(WEB, FINAL, by = c("date"="date", "user" = "Username", "start_time"="start_time"))

F_SL <- inner_join(FINAL, SLOG,  by = c("GUID"="USER_SID"))




