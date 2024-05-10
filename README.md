# 介绍
pymysqlbinlog 是一个使用python编写 用来离线**分析**/**解析**mysqlbinlog的工具.

支持MySQL 5.7/8.x 的**所有数据类型**



**主要功能**: 

1. **分析binlog**, 得到大事务信息, 表使用情况, DML使用情况等.
2. **解析binlog**, 解析binlog得到正向/回滚SQL.





# 特点

1. **简单方便**: 纯python3编写, 无依赖包
2. **安全**: 离线解析
3. **功能多**: 分析/解析binlog, 支持库/表/时间/pos/gtid等匹配
4. **支持范围广**: 支持mysql5.7/8.x 所有数据类型.
5. **实用**: 可做反向解析, 方便数据回滚.





# 使用

~~本工具后续应该不会提供二进制包.~~ 由于没有依赖包, 建议使用源码. 

## 下载

```shell
git clone git@github.com:ddcw/pymysqlbinlog.git
```



## 解析binlog

由于binlog可能没有记录数字类型的符号和字段名称, 且为离线解析, 所以SQL拼接可能并不能满足要求. 建议使用base64格式 (`--base64`)



### 正向解析

解析为`SQL`格式

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --sql
```

解析为`base64`格式

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --base64
```



### 解析为回滚SQL

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --base64 --rollback # 推荐
#python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --sql --rollback 
```

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --base64 --rollback --verbose=4
```



### 数据过滤

**时间过滤**

维持事务完整性. 使用QUEYR_EVENT(即BEGIN的时间来匹配)

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000040 --sql --start-datetime="2024-05-04 11:14:15" --stop-datetime="2024-05-04 11:14:23" 
```

**表名过滤**

事务可能不完整

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000040 --sql --table='sbtest1'
```

**库名过滤**

事务可能不完整

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000040 --sql --schema='db1'
```

**POS过滤**

维持事务完整性.  read_event的时候就匹配了POS信息

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000040 --sql --start-pos=780
```

**GTID过滤**

维持事务完整性. 若不匹配, 则进行下一个事务.

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000040 --sql --gtid 'b68e2434-cd30-11ec-b536-000c2980c11e'
```

**SERVERID过滤**

```shell
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000040 --sql --serverid=3314
```



由于有些环境GTID_MODE=ON, 则需求使用 `skip-gtids`选项, 来生成如下信息

```sql
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by ddcw pymysqlbinlog */ /*!*/;
```





## 分析binlog

可以分析多个BINLOG文件, 统计`TABLE`,`EVENT`,`TRX`三个维度的信息. 

由于TRX太多了, 不方便展示, 故只取 TOP20 大事务.  TABLE按照TOTAL_SIZE排序, 对于TABLE的COUNT表示行数.

(支持输出md格式.)

```shell
# 以TXT信息输出到屏幕(STDOUT)
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --analyze
```

或者

```shell
# 以MARKDOWN格式输出到指定文件
python3 main.py /data/mysql_3314/mysqllog/binlog/m3314.000027 --analyze -o t20240505.md
```

若为非md文件, 则为txt格式. 可以直接将结果复制到excel等工具查看, 还可以画图使结果更直观.

