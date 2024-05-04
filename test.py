from pymysqlbinlog.pymysqlbinlog import mysqlbinlog
import datetime
import sys

startdatetime = "20240416 16:26:49"
stopdatetime = "20240416 16:26:49"
time_format = "%Y%m%d %H:%M:%S"
start = int(datetime.datetime.strptime(startdatetime, time_format).timestamp())
stop = int(datetime.datetime.strptime(stopdatetime, time_format).timestamp())

if len(sys.argv) > 1:
	filename = sys.argv[1]
else:
	filename = "/data/mysql_3314/mysqllog/binlog/m3314.000144"
#filename = "/data/mysql_3314/mysqllog/binlog/m3314.000141"
#filename = '/data/mysql_3308/mysqllog/binlog/m3308.001242'
aa = mysqlbinlog(filename=filename)
#aa.DEBUG = True
#aa.ROLLBACK = True
aa.VERBOSE = 4
#aa.PRE_GTIDS_SKIP = False
#aa.FILTER_SCHEMA_EXCLUDE = 'db1'
#aa.FILTER_SCHEMA_INCLUDE = 'db1'
#aa.START_POSITION = 0
#aa.STOP_POSITION = 0
#aa.START_DATETIME = start
#aa.STOP_DATETIME = stop
#aa.FILTER_SERVERID_INCLUDE = '866003314'
#aa.FILTER_SERVERID_EXCLUDE = '866003314'
aa.init()
aa.test()
