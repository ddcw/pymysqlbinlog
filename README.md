```help
rollback  生成回滚SQL. 按照事务顺序 (读两次IO)
metadata  元数据信息的xml文件. 如果有的话, 则自动替换sql/sql2里面的字段名字
sql2      生成sql格式 (非注释), 由于数据类型转换可能存在问题, 建议使用base64格式
sql       生成sql格式 (注释的sql)
sql-complete 对于insert使用完整sql, 含字段名
sql-replace  使用replace替换insert和update
base64    生成base64格式(默认)
base64-disable 不要base64格式的数据,  如果没有sql/sql2, 则自动启动sql选项
debug     展示完整过程, 主要用于调试(stderr)
verbose   显示格外信息, 主要是注释信息. 比如binlog_rows_query_log_events

# 数据过滤, 优先匹配include, 匹配失败再匹配exclude, 匹配成功(返回False)则跳过
schema-include 同schema
schema-exclude
schema-replace 库名字替换, 所有符合要求的schema换为这个名字
table-include 同table
table-exclude
gtid-skip
gtid-include 同gtid
gtid-exclude
serverid-include
serverid-exclude
start-datetime
stop-datetime
start-position
stop-position
threadid-skip
threadid-exclude

# 审计相关(不统计过滤掉的event), 走stderr
analyze-event 基于event做统计, 各event类型的数量, 大小
analyze-table 基于表做统计     各表的大小, 各表的dml操作数量/行数/大小
analyze-trx   基于事务做统计   大事务(不含gtid event, 但起止pos含gtid和xid).
```
