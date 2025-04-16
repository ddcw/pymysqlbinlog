#!/usr/bin/env python3
# write by ddcw @https://github.com/ddcw

import argparse
import sys,os
import glob
from pymysqlbinlog import __version__
from pymysqlbinlog.pymysqlbinlog import mysqlbinlog
from pymysqlbinlog import event_type
import datetime

event_type_dict = {}
for n in dir(event_type):
	if n.find("EVENT") > -1:
		key = str(getattr(event_type,n))
		event_type_dict[key] = n

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'pymysqlbinlog/')))

def _argparse():
	parser = argparse.ArgumentParser(add_help=True, description='解析mysql8.0的ibd文件 https://github.com/ddcw/pymysqlbinlog')
	parser.add_argument('--version', action='store_true', dest="VERSION", default=False,  help='show version')
	parser.add_argument('--sql', action='store_true', dest="SQL", default=False,  help='输出SQL格式数据')
	parser.add_argument('--b64', '--base64', action='store_true', dest="BASE64", default=False,  help='输出base64格式数据(default)')
	parser.add_argument('--bin', action='store_true', dest="BINARY", default=False,  help='输出二进制格式数据(要求--output-file) ')
	parser.add_argument('--rollback', action='store_true', dest="ROLLBACK", default=False,  help='生成回滚数据, 和--sql/--b64/--bin一起使用')
	parser.add_argument('--debug', action='store_true', dest="DEBUG", default=False,  help='DEBUG')
	parser.add_argument('--analyze', action='store_true', dest="ANALYZE", default=False,  help='分析binlog, 可以支持多个Binlog文件, 和--sql/--b64/--bin 互斥')
	parser.add_argument('--verbose', action='store', dest="VERBOSE", default=0,  help='详细信息(对于--b64,会带有注释的SQL信息)')
	parser.add_argument('--output-file', '-o', '-O', action='store', dest="OUTPUT_FILE", default=None,  help='输出文件')

	# 下面部分就是binlog过滤了. 注意: 基于表的过滤 会破坏事务完整性
	parser.add_argument('--gtid-include', '--gtid', action='store', dest="GTID_INCLUDE", help='server_uuid require')
	parser.add_argument('--gtid-exclude', action='store', dest="GTID_EXCLUDE", help='server_uuid require')
	parser.add_argument('--gtid-skip',  '--skip-gtids',  action='store_true', dest="GTID_SKIP", help='不打印gtid信息')
	parser.add_argument('--serverid-include', '--serverid', action='store', dest="SERVERID_INCLUDE", help='serverid require')
	parser.add_argument('--serverid-exclude', action='store', dest="SERVERID_EXCLUDE", help='serverid require')
	parser.add_argument('--schema-include', '--schema', action='store', dest="SCHEMA_INCLUDE", help='schema require')
	parser.add_argument('--schema-exclude', action='store', dest="SCHEMA_EXCLUDE", help='schema require')
	parser.add_argument('--table-include', '--table', action='store', dest="TABLE_INCLUDE", help='table require')
	parser.add_argument('--table-exclude', action='store', dest="TABLE_EXCLUDE", help='table require')
	parser.add_argument('--start-datetime', action='store', dest="START_DATETIME", help='time require')
	parser.add_argument('--stop-datetime', action='store', dest="STOP_DATETIME", help='time require')
	parser.add_argument('--start-pos', action='store', dest="START_POS", help='pos require')
	parser.add_argument('--stop-pos', action='store', dest="STOP_POS", help='pos require')
	
	# 要解析的binlog文件, 如果是分析的话, 可以支持多个Binlog, 不然只支持1个binlog
	parser.add_argument(dest='FILENAME', help='binlog filename', nargs='*')
	
	if parser.parse_args().VERSION:
		print(f"pymysqlbinlog VERSION: v{__version__}")
		sys.exit(0)

	# 参数校验
	p = parser.parse_args()
	if (p.SQL and p.BASE64) or (p.SQL and p.BINARY) or (p.BINARY and p.BASE64) or (p.BINARY and p.BASE64 and p.SQL):
		print(f"--sql 和 --b64 和 --bin 是互斥的, 只能有一个生效")
		sys.exit(1)
	if p.ANALYZE and (p.BINARY or p.BASE64 or p.SQL):
		print(f"分析模式 和 解析模式是互斥的")
		sys.exit(1)
	if (not p.SQL) and (not p.BINARY) and not p.ANALYZE:
		p.BASE64 = True
	if p.BINARY and p.OUTPUT_FILE is None:
		print("--bin 要求 --output-file=xxxxxx.bin")
		sys.exit(1) # 文件是否存在就不校验了.

	# 检查Binlog文件是否存在
	filename = p.FILENAME
	_require = "至少要一个binlog文件:\nExample:\n\t pymysqlbinlog /data/binlog/mysql-binlog.0000032\n\tpython /data/binlog/mysql-binlog.000003* --analyze"
	if filename is None:
		print(_require)
		sys.exit(1)
	filelist = []
	for pattern in filename:
		filelist += glob.glob(pattern)
	filelist.sort()
	fileset = set(filelist)
	if not p.ANALYZE and len(fileset) > 1:
		print(f"仅支持解析一个binlog. /* 暂无多个Binlog解析要求 /*")
		sys.exit(1)
	if len(fileset) == 0:
		print(_require)
		sys.exit(1)
	md_flag = False
	if p.OUTPUT_FILE is not None and p.ANALYZE and p.OUTPUT_FILE[-3:] == ".md":
		md_flag = True

	return md_flag,fileset,p

def getpymysqlbinlogobj(p,filename):
	time_format = "%Y-%m-%d %H:%M:%S"
	aa = mysqlbinlog(filename=filename)
	aa.DEBUG = p.DEBUG
	aa.VERBOSE = int(p.VERBOSE)
	aa.ROLLBACK = p.ROLLBACK
	if p.START_DATETIME is not None:
		aa.START_DATETIME = int(datetime.datetime.strptime(p.START_DATETIME, time_format).timestamp())
	if p.STOP_DATETIME is not None:
		aa.STOP_DATETIME = int(datetime.datetime.strptime(p.STOP_DATETIME, time_format).timestamp())
	if p.START_POS is not None:
		aa.START_POSITION = int(p.START_POS)
	if p.STOP_POS is not None:
		aa.STOP_POSITION = int(p.STOP_POS)
	if p.GTID_INCLUDE is not None:
		aa.FILTER_GTID_INCLUDE = p.GTID_INCLUDE
	if p.GTID_EXCLUDE is not None:
		aa.FILTER_GTID_EXCLUDE = p.GTID_EXCLUDE
	if p.GTID_SKIP is not None:
		aa.GTID_SKIP = p.GTID_SKIP
	if p.SERVERID_INCLUDE is not None:
		aa.FILTER_SERVERID_INCLUDE = int(p.SERVERID_INCLUDE)
	if p.SERVERID_EXCLUDE is not None:
		aa.FILTER_SERVERID_EXCLUDE = int(p.SERVERID_EXCLUDE)
	if p.SCHEMA_INCLUDE is not None:
		aa.FILTER_SCHEMA_INCLUDE = p.SCHEMA_INCLUDE
	if p.SCHEMA_EXCLUDE is not None:
		aa.FILTER_SCHEMA_EXCLUDE = p.SCHEMA_EXCLUDE
	if p.TABLE_INCLUDE is not None:
		aa.FILTER_TABLE_INCLUDE = p.TABLE_INCLUDE
	if p.TABLE_EXCLUDE is not None:
		aa.FILTER_TABLE_EXCLUDE = p.TABLE_EXCLUDE
	return aa

def _tmd(title,header,table):
	data = f"\n\n# {title}\n\n"
	data += "|" + "|".join([ str(x) for x in header ]) + "|\n"
	data += "|" + "|".join([ "-" for x in header    ]) + "|\n"
	for _x in table:
		data += "|" + "|".join([ str(x) for x in _x ]) + "|\n"
	return data

if __name__ == '__main__':
	md_flag,filenames,parser = _argparse()
	if parser.ANALYZE:
		TRX = []
		EVENT = {}
		TABLE = {}
		for filename in filenames:
			print(f"ANALYZE FILE: {filename}")
			aa = getpymysqlbinlogobj(parser,filename)
			aa.init()
			a,b,c = aa.analyze()
			TRX += a
			for x in b:
				if x not in EVENT:
					EVENT[x] = b[x]
				else:
					EVENT[x]['SIZE']  += b[x]['SIZE']
					EVENT[x]['COUNT'] += b[x]['COUNT']
			for x in c:
				if x not in TABLE:
					TABLE[x] = c[x]
				else:
					for y in ['DELETE','INSERT','UPDATE']:
						TABLE[x][y]['SIZE']  += c[x][y]['SIZE']
						TABLE[x][y]['COUNT'] += c[x][y]['COUNT']
						TABLE[x][y]['ROWS']  += c[x][y]['ROWS']
		# 只取TOP20大事务, 不然太多了.
		TRX = sorted(TRX, key=lambda x: x[1])
		TRX = TRX[-20:]
		TRX.reverse()

		# TABLE 转为 2-d array 按照TOTAL_SIZE排序
		_TABLE = []
		for x in TABLE:
			_TABLE.append([x, TABLE[x]['INSERT']['SIZE']+TABLE[x]['DELETE']['SIZE']+TABLE[x]['UPDATE']['SIZE'], TABLE[x]['INSERT']['ROWS']+TABLE[x]['DELETE']['ROWS']+TABLE[x]['UPDATE']['ROWS'], TABLE[x]['INSERT']['SIZE'], TABLE[x]['INSERT']['ROWS'], TABLE[x]['DELETE']['SIZE'], TABLE[x]['DELETE']['ROWS'], TABLE[x]['UPDATE']['SIZE'], TABLE[x]['UPDATE']['ROWS'], ])
		TABLE = sorted(_TABLE, key=lambda x: x[1])
		TABLE.reverse()

		if md_flag:
			f = open(parser.OUTPUT_FILE,'a')
			f.write(_tmd("TOP20事务",['XID','事务大小(字节)',"起始偏移量","结束偏移量"],TRX))
			event_list = []
			for x in EVENT:
				event_list.append([event_type_dict[str(x)], EVENT[x]['SIZE'], EVENT[x]['COUNT']])
			f.write(_tmd("EVENT INFO",["EVENT NAME","EVENT总大小(字节)","EVENT数量"],event_list))
			f.write(_tmd("TABLE INFO",['TABLE_NAME','TOTAL_SIZE(bytes)','TOTAL_COUNT(rows)','INSERT_SIZE','INSERT_COUNT','DELETE_SIZE','DELETE_COUNT','UPDATE_SIZE','UPDATE_COUNT'],TABLE))
			f.close()
			print(f"\n请查看markdown文件:\t{parser.OUTPUT_FILE}\n")
		else:
			f = open(parser.OUTPUT_FILE,'a') if parser.OUTPUT_FILE is not None else sys.stdout
			# TRX INFO
			f.write("\n######### TOP20大事务 ############\nXID\tSIZE(bytes)\tSTART_POS\tSTOP_POS\n")
			for x in TRX:
				f.write(f"{x[0]}\t{x[1]}\t{x[2]}\t{x[3]}\n")

			# EVENT INFO
			f.write("\n\n######### EVENT INFO ############\nEVENT_NAME\tTOTAL_SIZE(bytes)\tTOTAL_COUNT\n")
			for x in EVENT:
				f.write(f"{event_type_dict[str(x)]}\t{EVENT[x]['SIZE']}\t{EVENT[x]['COUNT']}\n")

			# TABLE INFO
			f.write("\n\n######### TABLE INFO ############\nTABLE_NAME\tTOTAL_SIZE(bytes)\tTOTAL_COUNT(rows)\tINSERT_SIZE\tINSERT_COUNT\tDELETE_SIZE\tDELETE_COUNT\tUPDATE_SIZE\tUPDATE_COUNT\n")
			for x in TABLE:
				f.write('\t'.join([ str(y) for y in x])+"\n")
			f.close()
	else:
		aa = getpymysqlbinlogobj(parser,list(filenames)[0])
		if parser.OUTPUT_FILE is not None and not parser.BINARY:
			f = open(parser.OUTPUT_FILE,'w')
		elif parser.OUTPUT_FILE is not None:
			f = open(parser.OUTPUT_FILE,'wb')
		else:
			f = sys.stdout
		aa.outfd = f
		aa.init()
		if not parser.BINARY:
			aa.BASE64 = parser.BASE64
			aa.sql(parser.BASE64)
		else:
			aa.binary() 
		f.close()
