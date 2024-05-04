# 介绍
离线分析/解析 MYSQL BINLOG的工具, 使用纯python3编写, 无依赖包

# 使用
## 顺序解析为base64
```shell
python main.py /data/mysql_3314/mysqllog/binlog/m3314.000036 
```

## 顺序解析为SQL 

(无法识别符号和字段名字)

```shell
python main.py /data/mysql_3314/mysqllog/binlog/m3314.000036 --sql
```

## 回滚base64
```shell
python main.py /data/mysql_3314/mysqllog/binlog/m3314.000036 --base64 --rollback
python main.py /data/mysql_3314/mysqllog/binlog/m3314.000036 --base64 --rollback --verbose=4
```

## 回滚SQL 

(无法识别符号和字段名字)


## 分析binlog
```shell
python main.py /data/mysql_3314/mysqllog/binlog/m3314.000036 --analyze --output-file=test20240504.md
```
