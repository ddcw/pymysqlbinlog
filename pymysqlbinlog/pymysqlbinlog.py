#!/usr/bin/env python3
# write by ddcw @https://github.com/ddcw

import struct
import sys
import os
import datetime
import binascii
import base64
from pymysqlbinlog.event_type import *
from pymysqlbinlog.row_event import *
from pymysqlbinlog.event import event_header
from pymysqlbinlog.gtid_event import *
from pymysqlbinlog.query_event import query_event
from pymysqlbinlog.format_desc import format_desc_event

class mysqlbinlog(object):
	def __init__(self,*args,**kwargs):
		# 过滤规则
		self.FILTER_SCHEMA_INCLUDE    = None
		self.FILTER_SCHEMA_EXCLUDE    = None
		self.FILTER_TABLE_INCLUDE     = None
		self.FILTER_TABLE_EXCLUDE     = None
		self.FILTER_GTID_INCLUDE      = None
		self.FILTER_GTID_EXCLUDE      = None
		self.FILTER_SERVERID_INCLUDE  = None
		self.FILTER_SERVERID_EXCLUDE  = None
		self.FILTER_THREAD_INCLUDE    = None
		self.FILTER_THREAD_EXCLUDE    = None

		# 起止时间/位置, 均为int (虽然叫datetime,但实际为Int -_-)
		self.START_DATETIME = None    # 左闭右开. start <= date < stop
		self.START_POSITION = None    # format event 始终会读取 (if not sql/sql2)
		self.STOP_DATETIME  = None
		self.STOP_POSITION  = None

		# 审计相关
		self.ANALYZE_EVENT  = False
		self.ANALYZE_TABLE  = False
		self.ANALYZE_TRX    = False
		self.EVENT_DICT     = {}      # 待会再初始化. 所有event类型都有
		self.TABLE_DICT     = {}
		self.TRX_LIST       = []
		
		# 输出相关信息
		self.SCHEMA_REPLACE = None
		self.VERBOSE        = 0       # 输出更多信息,STDERR
		self.GTID_SKIP      = False
		self.ROLLBACK       = False
		self.BASE64         = False   # 数据以base64格式输出
		self.SQL            = False   # 数据以注释的SQL格式输出
		self.SQL2           = False   # 数据以非注释的SQL格式输出 与SQL冲突, 仅一个为True, 都为True时, 仅SQL有效
		self.DEBUG          = False   # DEBUG. 直接写stderr, 不需要指定文件
		self.SQL_COMPLETE   = False   # insert显示完整的字段信息. 如果有metadata的话, 可以设置一下
		self.SQL_REPLACE    = False   # 使用replace 替换insert/update
		self.SQL_HEX        = False   # 对于二进制的数据使用 0xE6B  代替 _binary ''

		# 基础信息
		self.PRE_GTIDS_SKIP = True    # 跳过PREVIOUS_GTIDS_LOG_EVENT
		self.METADATA       = None    # 元数据信息, 字段名字
		self.filename       = kwargs['filename']
		self.outfd          = sys.stdout #可定义输出文件描述符, 默认输出到stdout
		self.outfd2         = sys.stderr #debug信息输出fd
		self.checksum       = False   # 是否有结尾的校验值. 返回数据的时候, 不包含结尾的checksum. 但是offset信息会显示
		self.mysql_version  = ""
		self._pos           = 0
		self.DELIMITER      = "/*!*/;"
		self.BINARY         = False   # 写新的文件
		self._bdata_pre_gtid = b''

	def debug(self,*args):
		if self.DEBUG:
			msg = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [DEBUG] {' '.join([ str(x) for x in args ])}\n"
			#sys.stderr.write(msg)
			self.outfd2.write(msg)

	def write(self,*args):
		self.outfd.write(f"{' '.join([ str(x) for x in args ])}\n")


	def _filter_server_id(self,serverid):
		#serverid = str(serverid)
		# 先不管正则表达式了, 后面再说
		if self.FILTER_SERVERID_INCLUDE is not None:# and self.FILTER_SERVERID_INCLUDE == serverid:
			return True if self.FILTER_SERVERID_INCLUDE == serverid else False
		if self.FILTER_SERVERID_EXCLUDE is not None:
			return False if self.FILTER_SERVERID_EXCLUDE == serverid else True
		return True

	def _filter_datetime(self,dt):
		ok1 = True
		ok2 = True
		if self.START_DATETIME is not None:
			ok1 =  True if dt >= self.START_DATETIME else False
		if self.STOP_DATETIME is not None:
			ok2 = False if dt > self.STOP_DATETIME else True
		return True if ok1 and ok2 else False

	def _filter_pos(self,start_pos,stop_pos):
		ok1 = True
		ok2 = True
		if self.START_POSITION is not None:
			ok1 = True if start_pos >= self.START_POSITION else False
		if self.STOP_POSITION is not None:
			ok2 = False if stop_pos >= self.STOP_POSITION else True
		return True if ok1 and ok2 else False

	def _filter_dbname(self,dbname):
		if self.FILTER_SCHEMA_INCLUDE is not None:# and dbname == self.FILTER_SCHEMA_INCLUDE:
			return True if dbname == self.FILTER_SCHEMA_INCLUDE else False
		if self.FILTER_SCHEMA_EXCLUDE is not None:# and dbname == self.FILTER_SCHEMA_EXCLUDE:
			return False if dbname == self.FILTER_SCHEMA_EXCLUDE else True
		return True

	def _filter_tablename(self,tablename):
		if self.FILTER_TABLE_INCLUDE is not None:# and tablename == self.FILTER_TABLE_INCLUDE:
			return True if tablename == self.FILTER_TABLE_INCLUDE else False
		if self.FILTER_TABLE_EXCLUDE is not None:# and tablename == self.FILTER_TABLE_EXCLUDE:
			return False if tablename == self.FILTER_TABLE_EXCLUDE else True
		return True

	def _filter_gtid(self,gtid):
		if self.FILTER_GTID_INCLUDE is not None:# and self.FILTER_GTID_INCLUDE == gtid:
			return True if self.FILTER_GTID_INCLUDE == gtid else False
		if self.FILTER_GTID_EXCLUDE is not None:# and self.FILTER_GTID_EXCLUDE == gtid:
			return False if self.FILTER_GTID_EXCLUDE == gtid else True
		return True

	def read_trx_event(self):
		"""
		返回一个事务(list), 若使用schema/table过滤, 则可能破坏事务的完整性.
		start/stop datetime 均按照gtid的时间来判断, 还是维持左闭又开(]
		当匹配到QUERY_EVENT/XID_EVENT 则做退出判断, 如果是空事务(schema/table匹配失败), 则继续
		"""
		self.debug("READ TRX BEGIN")
		trx_list = []
		NOT_HAVE_TRX = True
		HAVE_GTID = False
		HAVE_DML = False
		START_POS = 0
		TRX_BROKEN = False # 事务是否完整 False: 完整  True:不完整
		while NOT_HAVE_TRX:
			try:
				_startpos = self.f.tell()
			except:
				break
			self._pos = _startpos
			bdata = self.read_event()
			if bdata == b'':
				trx_list = [] #事务未结束, 就不要返回了
				break
			event_type = struct.unpack('>B',bdata[4:5])[0]
			#self.debug(f"EVENT TYPE: {event_type}")
			if event_type in [ GTID_LOG_EVENT, ANONYMOUS_GTID_LOG_EVENT ]: #gtid_event
				# 匹配GTID
				aa = gtid_event(bdata=bdata[19:], debug=self.debug,verbose=self.VERBOSE)
				aa.init()
				if not self._filter_gtid(aa.SID):
					trx_list = []
					if self.VERBOSE >=2:
						self.debug(f"SKIP GTID:{aa.SID} {aa.GNO}")
					continue
				# 匹配时间. 使用事务开始时间, 而不是original_commit_timestamp
				#_dt = int(aa.original_commit_timestamp/1000000)
			#	_dt = struct.unpack('<L',bdata[:4])[0]
			#	# 感觉还是使用QUERY TIME好点... 
			#	if not self._filter_datetime(_dt):
			#		trx_list = []
			#		if self.VERBOSE >=2:
			#			self.debug(f"SKIP GTID:{aa.SID} {aa.GNO}")
			#		continue
				
				START_POS = _startpos
				trx_list = []
				HAVE_GTID = True
			elif event_type == XID_EVENT: # 校验这个事务 是否符合时间, 不符合则继续
				# 匹配结束时间
				#if self.STOP_DATETIME is not None and struct.unpack('>L',bdata[:4])[0] <= self.STOP_DATETIME:
				#	trx_list = []
				#	TRX_BROKEN = False
				#	continue
				#	
				if len(trx_list) > 1 and HAVE_DML and HAVE_GTID:
					NOT_HAVE_TRX = False
					if self.VERBOSE >= 1:
						self.debug(f"READ TRX FINISH. count event: {len(trx_list)+1}  offset:{START_POS} --> {self.f.tell()}")
				else:
					trx_list = []
					TRX_BROKEN = False
				HAVE_GTID = False
				HAVE_DML = False
			elif event_type == TABLE_MAP_EVENT:
				# 匹配数据库名字和表名字
				tme = tablemap_event(bdata=bdata[19:],debug=self.debug,verbose=self.VERBOSE,event_header=bdata[:19])
				tme.init()
				if self.VERBOSE >=2:
					self.debug(f"TABLE_NAME: `{tme.dbname}`.`{tme.table_name}`")
				if self._filter_tablename(tme.table_name) and self._filter_dbname(tme.dbname):
					#print("XXXXXXXXXXX",tme.table_name,tme.dbname, self.FILTER_TABLE_INCLUDE,self.FILTER_TABLE_EXCLUDE,self.FILTER_SCHEMA_INCLUDE,self.FILTER_SCHEMA_EXCLUDE)
					HAVE_DML = True
					trx_list.append(bdata)
					trx_list.append(self.read_event())
					continue
				else:
					if self.VERBOSE >= 2:
						self.debug(f"`{tme.dbname}`.`{tme.table_name}` is not match. skip it")
					#TRX_BROKEN = True # 表不匹配就不吧, 破坏事务的完整性了.
					#trx_list = [] 
				# 替换数据库名字(TODO)
			elif event_type == ROWS_QUERY_LOG_EVENT:
				if self.VERBOSE >= 1:
					self.debug("ADD ROWS_QUERY_LOG_EVENT")
				#print(bdata)
				HAVE_DML = True # 虽然叫DML, 实际上包含了DDL. 这名字取得...
			elif event_type == QUERY_EVENT: #DDL, such as: BEGIN
				if self.VERBOSE >= 1:
					self.debug("ADD QUERY EVENT")
				#trx_list.append(bdata)
				if bdata[-5:] != b'BEGIN': #BEGIN就没必要加了. 
					NOT_HAVE_TRX = False
				#if bdata[-5:] == b"BEGIN":
				else:
					_dt = struct.unpack('<L',bdata[:4])[0]
					if not self._filter_datetime(_dt):
						NOT_HAVE_TRX = True
						HAVE_GTID = False
					
				#break
			else: #除了上面几种event, 其它的就不要出现在trx里面了.
				if self.VERBOSE >= 1:
					self.debug(f"SKIP EVENT TYPE:{event_type}")
				continue
			trx_list.append(bdata)
		if self.ROLLBACK:
			trx_list.reverse()
			if len(trx_list) > 0 and struct.unpack('>B',trx_list[0][4:5])[0] == XID_EVENT : #XID EVENT 则把BEGIN,COMMIT顺序换回去
				trx_list[0],trx_list[-1] = trx_list[-1],trx_list[0]
		self.debug(f"READ TRX FINISH. {len(trx_list)}")
		return trx_list

	def read_event(self,skipn=0):
		# skipn 表示 跳过的event数量. 主要是用作过滤. 不含已过滤掉的event
		"""过滤掉一部分不符合要求的event, 并返回event二进制数据(header&payload) except checksum"""
		bdata = b''
		start_pos = 0
		stop_pos  = 0
		event_type = UNKNOWN_EVENT
		if self.VERBOSE >= 1:
			self.debug(f"READ EVENT BEGIN")
		while skipn >= 0:
			skipn -= 1
			start_pos = self.f.tell()
			bdata = self.f.read(19)
			if bdata == b'':
				self.f.close()
				return b''
			timestamp, event_type, server_id, event_size, log_pos, flags = struct.unpack("<LBLLLh",bdata[:19])
			#bdata_payload = self.f.read(event_size-19)
			bdata += self.f.read(event_size-19)
			stop_pos = self.f.tell()

			# 如果是format event 或者pre gtid 则直接打印相关信息
			if event_type == FORMAT_DESCRIPTION_EVENT:
				self._bdata_format_desc = bdata
				self._format_event_b64 = f"BINLOG '\n{self._base64(self._bdata_format_desc)}'{self.DELIMITER}\n"
				if self.BASE64:
					self.outfd.write(self._format_event_b64)
				self.format_desc = format_desc_event(bdata=self._bdata_format_desc[19:],debug=self.debug)
				self.format_desc.init()
				self.checksum = self.format_desc.checksum
				self.mysql_version = self.format_desc.mysql_version
				skipn += 1
				continue

			# 跳过 PREVIOUS_GTIDS_LOG_EVENT
			if event_type == PREVIOUS_GTIDS_LOG_EVENT :
				self._bdata_pre_gtid = bdata
				self.pre_gtid = pre_gtid_event(bdata=bdata[19:], debug=self.debug)
				self.pre_gtid.init()
				skipn += 1
				continue
			#	if self.VERBOSE >= 2:
			#		self.debug("SKIP EVENT: PREVIOUS_GTIDS_LOG_EVENT")
			#	continue

			# 条件过滤 server_id
			if not self._filter_server_id(server_id):
				if self.VERBOSE >= 2:
					self.debug(f"server_id {server_id} is not match. will skip it. {start_pos} --> {stop_pos}")
				skipn += 1 #匹配失败, 则再循环一次
				continue

			# 条件过滤 时间, 匹配失败则退出 (放到事务里面去)
			#if not self._filter_datetime(timestamp):
			#	if self.VERBOSE >= 2:
			#		self.debug(f"timestamp {timestamp} is not match. will break")
			#	return b''

			# 条件过滤 pos
			if not self._filter_pos(start_pos, stop_pos):
				if self.VERBOSE >= 2:
					self.debug(f"position  {start_pos} --> {stop_pos} is not match. will break.")
				#return b''
				skipn += 1
				continue

		if self.VERBOSE >= 1:
			self.debug(f"READ EVENT FINISH. EVENT TYPE:{event_type} {start_pos} --> {stop_pos}  size:{int(stop_pos-start_pos)} bytes")
		if self.VERBOSE >= 2 and self.checksum:
			c32checksum = binascii.crc32(bdata[:-4])
			self.debug(f"CRC32:{c32checksum}({hex(c32checksum)})  CHECKSUM:{struct.unpack('<L',bdata[-4:])[0]}")
		if self.checksum:
			return bdata[:-4]
		else:
			return bdata
			

	def init(self,):
		"""初始化, 如果成功,则返回True"""
		# 检查文件是否存在(不检查了. main.py检测了的)
		# 检查能否打开文件
		# 检查文件格式(不检测了. 直接报错抛异常好点)
		self.debug(f"INIT FILENAME: {self.filename}")
		self.f = open(self.filename,'rb')
		relaylog_flag = False
		if self.f.read(4) != b'\xfebin':
			relaylog_flag = True
			self.debug(f"{self.filename} maybe relay log")
			self.f.seek(0,0) # relay log不用跳过magic
		event = self.read_event() # 初始化一下FORMAT_EVENT和PREGTID EVENT
		if event == b'':
			self.debug("NO EVENT")
		else:
			_offset = 0 if relaylog_flag else 4
			self.f.seek(_offset,0)
		return True

	#def analyze(self):
	#	""" 统计binlog信息 返回统计结果. 方便后续分析""" 
	#	pass

	def rollback(self, b64=True):
		""" 生成回滚SQL 默认为base64"""
		if b64:
			self.BASE64 = True
		pass # 生成回滚SQL

	def _base64(self,bdata):
		""" 生成base64的数据 """
		rdata = ''
		for x in range(0,len(bdata),57):
			rdata += base64.b64encode(bdata[x:x+57]).decode() + "\n"
		return rdata

	def base64(self,):
		""" 生成base64数据"""
		return self.sql(True)

	def sql(self,TOBASE64=False):
		""" 生成带有注释或者不带注释的SQL """
		# self.checksum 
		TRX = []
		if self.ROLLBACK:
			while True:
				trx = self.read_trx_event()
				if len(trx) == 0:
					break
				TRX.append(trx)
		TRX.reverse() # 两级反转
		if not self.BINARY:
			#print(self.BINARY)
			self.outfd.write(f"DELIMITER {self.DELIMITER}\n")
		if TOBASE64 or self.BINARY:
			#self.outfd.write(self._format_event_b64)
			pass
		else:
			self.outfd.write(f"ROLLBACK  {self.DELIMITER}\n")
		def __gettrx():
			if self.ROLLBACK:
				for _x in TRX:
					yield _x
			else:
				while True:
					_x = self.read_trx_event()
					yield _x
		gentrx = __gettrx()
		for _x in gentrx:
			if len(_x) == 0:
				break
			_rowevent = []
			for x in _x:
				event_type = struct.unpack('>B',x[4:5])[0]
				if self.BINARY and (event_type not in(TABLE_MAP_EVENT,WRITE_ROWS_EVENT,UPDATE_ROWS_EVENT,DELETE_ROWS_EVENT)):
					self.write_event(x)
					continue
				if event_type == ANONYMOUS_GTID_LOG_EVENT :
					self.outfd.write(f"SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by ddcw pymysqlbinlog */ {self.DELIMITER}\n")
				if event_type == GTID_LOG_EVENT:
					if not self.GTID_SKIP and not self.ROLLBACK:
						aa = gtid_event(bdata=x[19:])
						aa.init()
						self.outfd.write(f"SET @@SESSION.GTID_NEXT= '{aa.SID}:{aa.GNO}' {self.DELIMITER}\n")
					elif self.ROLLBACK:
						self.outfd.write(f"SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by ddcw pymysqlbinlog */ {self.DELIMITER}\n")
					else:
						self.outfd.write(f"SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by ddcw pymysqlbinlog */ {self.DELIMITER}\n")
					
				elif event_type == XID_EVENT:
					self.outfd.write(f"COMMIT /* {struct.unpack('<Q',x[19:19+8])[0]} added by ddcw pymysqlbinlog*/ {self.DELIMITER}\n\n")
				elif event_type == QUERY_EVENT:
					aa = query_event(bdata=x[19:],verbose=self.VERBOSE)
					if self.VERBOSE:
						aa.debug2 = self.debug
					aa.init()
					if (self.ROLLBACK or TOBASE64) and aa.query[-5:] != "BEGIN": # rollback 不做DDL
						continue
					# 设置初始化信息 , 整上..
					# @@sql_mode @@auto_increment_increment @@auto_increment_offset @@character_set_client @@collation_connection @@collation_server @@lc_time_names @@collation_database @@default_collation_for_utf8mb4
					# 输出时间戳
					self.outfd.write(f"SET TIMESTAMP={struct.unpack('<L',x[:4])[0]} /* QUERY TIME {str(datetime.datetime.fromtimestamp(struct.unpack('<L',x[:4])[0]))} */ {self.DELIMITER}\n")
					if "Q_SQL_MODE_CODE" in aa.status_vars_dict:
						self.outfd.write(f"SET @@session.sql_mode={aa.status_vars_dict['Q_SQL_MODE_CODE']} {self.DELIMITER}\n")
					if "Q_AUTO_INCREMENT" in aa.status_vars_dict:
						self.outfd.write(f"SET @@session.auto_increment_increment={aa.status_vars_dict['Q_AUTO_INCREMENT'][0]}, @@session.auto_increment_offset={aa.status_vars_dict['Q_AUTO_INCREMENT'][1]} {self.DELIMITER}\n")
					if "Q_CHARSET_CODE" in aa.status_vars_dict:
						self.outfd.write(f"SET @@session.character_set_client={aa.status_vars_dict['Q_CHARSET_CODE'][0]}, @@session.collation_connection={aa.status_vars_dict['Q_CHARSET_CODE'][1]}, @@session.collation_server={aa.status_vars_dict['Q_CHARSET_CODE'][2]} {self.DELIMITER}\n")
					if "Q_DEFAULT_COLLATION_FOR_UTF8MB4" in aa.status_vars_dict:
						self.outfd.write(f"/*!80011 SET @@session.default_collation_for_utf8mb4={aa.status_vars_dict['Q_DEFAULT_COLLATION_FOR_UTF8MB4']}*/{self.DELIMITER}\n")
					if aa.dbname != '':
						self.outfd.write(f"USE {aa.dbname} {self.DELIMITER}\n")
					elif "Q_UPDATED_DB_NAMES" in aa.status_vars_dict:
						self.outfd.write(f"USE {aa.status_vars_dict['Q_UPDATED_DB_NAMES']} {self.DELIMITER}\n")
					if aa.query[-17:] == "START TRANSACTION": # 8021 引入的 CREATE TABLE ... SELECT
						self.outfd.write(f"{aa.query[:-17]}{self.DELIMITER}\n")
					else:
						self.outfd.write(f"{aa.query}{self.DELIMITER}\n")
					if 'Q_DDL_LOGGED_WITH_XID' in aa.status_vars_dict:
						self.outfd.write(f"COMMIT /* XID {aa.status_vars_dict['Q_DDL_LOGGED_WITH_XID']} added by ddcw pymysqlbinlog*/ {self.DELIMITER}\n\n")
				elif event_type in [ WRITE_ROWS_EVENT,UPDATE_ROWS_EVENT,DELETE_ROWS_EVENT ]:
					if self.ROLLBACK:
						_rowevent.append(x)
					else:
						aa = row_event(bdata=x[19:],debug=self.debug,tablemap=self.table_map,event_header=x[:19],verbose=self.VERBOSE)
						aa.checksum = self.checksum
						aa.rollback = False
						aa.init()
						if self.BINARY:
							_tablemapevent,_rowevent = aa.read_bdata()
							if self.checksum:
								self.write_event(_tablemapevent[:-4])
								self.write_event(_rowevent[:-4])
							else:
								self.write_event(_tablemapevent)
								self.write_event(_rowevent)
							continue
						if not TOBASE64:
							for sql1 in aa.read_sql():
								self.outfd.write(f"{sql1}{self.DELIMITER}\n")
						else:
							self.outfd.write(f"{aa.read_b64()}{self.DELIMITER}\n")
							if self.VERBOSE == 4:
								for sql1 in aa.read_sql():
									self.outfd.write(f"-- {sql1}{self.DELIMITER}\n")
				elif event_type == TABLE_MAP_EVENT:
					tablemap = tablemap_event(bdata=x[19:],event_header=x[:19])
					tablemap.init()
					if self.ROLLBACK:
						for _y in _rowevent:
							aa = row_event(bdata=_y[19:],debug=self.debug,verbose=self.VERBOSE,tablemap=tablemap,event_header=_y[:19])
							aa.checksum = self.checksum
							aa.rollback = True
							aa.init()
							if self.BINARY:
								_tablemapevent,_rowevent = aa.read_bdata()
								self.write_event(_tablemapevent)
								self.write_event(_rowevent)
								continue
							if not TOBASE64:
								for sql1 in aa.read_sql():
									self.outfd.write(f"{sql1}{self.DELIMITER}\n")
							else:
								self.outfd.write(f"{aa.read_b64()}{self.DELIMITER}\n")
								if self.VERBOSE == 4:
									for sql1 in aa.read_sql():
										self.outfd.write(f"-- {sql1}{self.DELIMITER}\n")
						_rowevent = []
					else:
						self.table_map = tablemap
		if not self.BINARY:
			self.outfd.write(f"DELIMITER ;\n")

	def write_event(self,event):
		# 修改偏移量(event_size不需要修改, 因为不会变), 做CRC32校验, 然后写入BINLOG FILE
		timestamp, event_type, server_id, event_size, log_pos, flags = struct.unpack("<LBLLLh",event[0:19])
		if self.checksum:
			event_size = len(event) + 4
		else:
			event_size = len(event)
		current_offset = self.outfd.tell()
		log_pos = current_offset + event_size
		event_header = struct.pack("<LBLLLh", timestamp, event_type, server_id, event_size, log_pos, flags)
		event = event_header + event[19:]
		if self.checksum:
			event = event + struct.pack('<L',binascii.crc32(event))
		self.outfd.write(event)
		if self.VERBOSE >=4:
			self.debug(f"REWRITE EVENT {event_type} FINISH")

	def binary(self):
		""" 生成二进制的 """
		_tmpstat = self.BINARY
		_tmpstat2 = self.BASE64
		self.BINARY = True
		self.BASE64 = False
		# 写BINLOG MAGIC
		self.outfd.write(b'\xfebin')
		# 写FORMAT EVENT and PREGTID EVENT
		if self.checksum:
			self.write_event(self._bdata_format_desc[:-4])
		else:
			self.write_event(self._bdata_format_desc)
		# 写PREGTID EVENT
		if self._bdata_pre_gtid != b'':
			if self.checksum:
				self.write_event(self._bdata_pre_gtid[:-4])
			else:
				self.write_event(self._bdata_pre_gtid)
		# 写剩余的数据
		self.sql()
		self.BINAEY = _tmpstat
		self.BASE64 = _tmpstat2
		# 写ROTATE_EVENT
		# 算了...
		

	def analyze(self):
		"""分析binlog, 返回分析后的数据."""
		TABLE = {} #{"DELETE":{"SIZE":0, "COUNT":0, "ROWS":0}, "INSERT":{"SIZE":0, "COUNT":0, "ROWS":0}, "UPDATE":{"SIZE":0, "COUNT":0, "ROWS":0}} 
		DATABASE = {"DELETE":{"SIZE":0, "COUNT":0, "ROWS":0}, "INSERT":{"SIZE":0, "COUNT":0, "ROWS":0}, "UPDATE":{"SIZE":0, "COUNT":0, "ROWS":0}}
		TRX = []  # XID, SIZE, START_POS, STOP_POS
		EVENT = {} # XID_EVENT:[0,0]  #size count
		while True:
			start_pos = self._pos
			trx = self.read_trx_event()
			stop_pos = self._pos
			if len(trx) == 0:
				break
			tbname = ""
			xid = -1
			for x in trx:
				event_type = struct.unpack('>B',x[4:5])[0]
				if event_type not in EVENT:
					EVENT[event_type] = {"SIZE":0,"COUNT":0}
				EVENT[event_type]["SIZE"] += len(x)
				EVENT[event_type]["COUNT"] += 1
				if event_type in [ GTID_LOG_EVENT, ANONYMOUS_GTID_LOG_EVENT ]:
					pass
				elif event_type == TABLE_MAP_EVENT:
					aa  = tablemap_event(bdata=x[19:],event_header=x[:19])
					aa.init()
					tbname = f"{aa.dbname}.{aa.table_name}"
					if tbname not in TABLE:
						TABLE[tbname] = {"DELETE":{"SIZE":0, "COUNT":0, "ROWS":0}, "INSERT":{"SIZE":0, "COUNT":0, "ROWS":0}, "UPDATE":{"SIZE":0, "COUNT":0, "ROWS":0}}
					self.table_map = aa
				elif event_type in [ WRITE_ROWS_EVENT,UPDATE_ROWS_EVENT,DELETE_ROWS_EVENT ]:
					aa = row_event(bdata=x[19:],tablemap=self.table_map,event_header=x[:19],verbose=self.VERBOSE)
					aa.checksum = True
					aa.init()
					if event_type == WRITE_ROWS_EVENT:
						_tname = "INSERT"
					if event_type == UPDATE_ROWS_EVENT:
						_tname = "UPDATE"
					if event_type == DELETE_ROWS_EVENT:
						_tname = "DELETE"
					TABLE[tbname][_tname]["SIZE"]  += len(aa.bdata) + len(aa.tablemap.bdata)
					TABLE[tbname][_tname]["COUNT"] += 1
					TABLE[tbname][_tname]["ROWS"]  += len(aa.row)
				elif event_type == XID_EVENT:
					xid = struct.unpack('<Q',x[19:19+8])[0]
				elif event_type == QUERY_EVENT:
					aa = query_event(bdata=x[19:])
					aa.init()
					try:
						xid = aa.status_vars_dict['Q_DDL_LOGGED_WITH_XID']
					except:
						pass
				elif event_type == ROWS_QUERY_LOG_EVENT:
					pass
			TRX.append([xid,stop_pos-start_pos,start_pos,stop_pos])
		return TRX,EVENT,TABLE

