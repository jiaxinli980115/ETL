library(tidyverse)
library(RPostgres)
library(data.table)

#CREATE THE DATABASE----
#_create a connection to kindle database----
#remotely
con <- dbConnect(
  drv = dbDriver('Postgres'), 
  dbname = 'kindle',
  host = 'db-postgresql-nyc1-44203-do-user-8018943-0.b.db.ondigitalocean.com', 
  port = 25060,
  user = 'doadmin', 
  password = 'ayxhea1w79p562zx'
)

#_create custs table----
# REPLACE UNI with your UNI!
stmt <- 'CREATE TABLE custs_jl5681 (
          cust_id int,
          cust_name varchar(50),
          email_add varchar(50),
          PRIMARY KEY(cust_id)
        );'
dbExecute(con, stmt)  

#_create books table----
# REPLACE UNI with your UNI!
stmt <- 'CREATE TABLE books_jl5681 (
          book_id int,
          book_title varchar(100),
          book_category varchar(50),
          book_author varchar(50),
          PRIMARY KEY(book_id)
        );'
dbExecute(con, stmt)  

#_create book_reads table----
# REPLACE UNI with your UNI!
stmt <- 'CREATE TABLE book_reads_jl5681 (
          read_id int,
          cust_id int,
          book_id int,
          date_time timestamp,
          status varchar(5),
          PRIMARY KEY(read_id),
          FOREIGN KEY(cust_id) REFERENCES custs_jl5681,
          FOREIGN KEY(book_id) REFERENCES books_jl5681
        );'
dbExecute(con, stmt)  

#******************----
#EXTRACT----
#_read source data----
df <- read.csv('Kindle_Data.csv')


#_keep only customers with email addresses ending in .com----
df <- df[grepl('\\com$',df$email_add),]


#******************----
#TRANSFORM----
#_gather the begin and end times----
# Place each in a column called 'date_time'
#   along with a new column called 'status' which
#   will contain either 'begin' or 'end'.


df$status <- 'begin'

df1 <- df %>% 
  select(cust_name,email_add,book_title,book_category,book_author,begin_date_time,status)

df2 <- df %>% filter(end_date_time != "") %>% select(cust_name,email_add,book_title,book_category,book_author,end_date_time,status)
df2$status <- 'end'

names(df1)[6] = "date_time"
names(df2)[6] = "date_time"

df <- rbind

#******************----
#LOAD----
#_load custs_UNI table----
df1 <- df %>%
  select(cust_name, email_add) %>%
  distinct()

df1c <- bind_cols('cust_id' = 1:nrow(df1), df1)
dbWriteTable(
  con, 'custs_jl5681', df1c,
  row.names = FALSE, append = TRUE
)
 


 
#_load books_UNI table----

df1 <- df %>%
  select(book_title,book_category,book_author) %>%
  distinct()
df1b <- bind_cols('book_id' = 1:nrow(df1), df1)
dbWriteTable(
  con, 'books_jl5681', df1b,
  row.names = FALSE, append = TRUE
)

#_load book_reads_UNI table----

df1 <- df %>% 
  select(cust_name,book_title,date_time,status)%>%
  distinct()

df1br <- df1%>%
  inner_join(df1b) %>%
  inner_join(df1c) %>%
  select(cust_id, book_id,date_time,status)
df2br <- bind_cols('read_id' = 1:nrow(df1br), df1br)
dbWriteTable(
  con, 'book_reads_jl5681', df2br,
  row.names = FALSE, append = TRUE 
)

#******************----
#END----