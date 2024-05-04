#@libbinlogevents/include/rows_event.h
# https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Table__map__event.html
# 描述和格式都有. (网站那个不全, 源码里面更全. 有些类型没有及时更新到注释文档里面.)
# 都是对拼接为SQL有帮助的. 好家伙, 都快能出DDL了.

#符号 只对number类型有效, 还是从左到右, 1bit表示1个number, 1表示无符号, 0表示有符号
IGNEDNESS = 1

# 字符集
DEFAULT_CHARSET = 2

# 库字符集
COLUMN_CHARSET = 3

# 字段名字.(only for binlog_row_metadata=FULL) 字段名长度限制为64字节
COLUMN_NAME = 4

# set的值 binlog_row_metadata=FULL
SET_STR_VALUE = 5

# enum的值 binlog_row_metadata=FULL
ENUM_STR_VALUE = 6

# 空间坐标的 binlog_row_metadata=FULL
GEOMETRY_TYPE = 7

# 主键 binlog_row_metadata=FULL
SIMPLE_PRIMARY_KEY = 8
# 主键前缀索引  binlog_row_metadata=FULL
PRIMARY_KEY_WITH_PREFIX = 9
ENUM_AND_SET_DEFAULT_CHARSET = 10
ENUM_AND_SET_COLUMN_CHARSET = 11

# 可见字段
COLUMN_VISIBILITY = 12
