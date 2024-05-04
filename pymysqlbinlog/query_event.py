# write by ddcw @https://github.com/ddcw
from pymysqlbinlog.event import event
from pymysqlbinlog.event import event_header
from pymysqlbinlog.event_type import *
import struct

class tmpbuffer(object):
	def __init__(self,bdata):
		self.offset = 0
		self.bdata = bdata
	def read(self,n):
		data = self.bdata[self.offset:self.offset+n]
		self.offset += n
		return data

	def read_uint(self,n):
		return int.from_bytes(self.read(n),'little',signed=False)

	def read_string(self,flag):
		rdata = b''
		while True:
			data = self.read(1)
			if data == b'' or data == flag:
				break
			rdata += data
		return rdata

class query_event(event):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)

	def init(self,):
		self.thread_id = self.read_uint(4)
		self.query_exec_time = self.read_uint(4)
		db_len = self.read_uint(1)
		self.error_code = self.read_uint(2)
		status_vars_len = self.read_uint(2)
		self.status_vars = self.read(status_vars_len)
		self.dbname = self.read(db_len).decode()
		self.read(1) #b'\x00'
		#self.query = self.read(9999999)
		self.query = self.read(len(self.bdata)-self.offset).decode()
		if self.verbose >= 3:
			self.debug(f"thread_id        : {self.thread_id}")
			self.debug(f"query_exec_time  : {self.query_exec_time}")
			self.debug(f"error_code       : {self.error_code}")
			self.debug(f"status_vars      : {self.status_vars}")
			self.debug(f"dbname           : {self.dbname}")
			self.debug(f"DDL              : {self.query}")
	
		# status_vars 解析
		# @libbinlogevents/include/statement_events.h
		# @libbinlogevents/src/statement_events.cpp
		self.status_vars_dict = {}
		bf = tmpbuffer(self.status_vars)
		while True:
			t = bf.read(1)
			if t == b'':
				break
			tp = struct.unpack('<B',t)[0]
			if tp == 0:
				self.status_vars_dict['Q_FLAGS2_CODE'] = bf.read_uint(4)
			elif tp == 1:
				self.status_vars_dict['Q_SQL_MODE_CODE'] = bf.read_uint(8)
			elif tp == 2:
				pass
				self.status_vars_dict['Q_CATALOG_CODE'] = bf.read_uint(bf.read_uint(1))
			elif tp == 3:
				self.status_vars_dict['Q_AUTO_INCREMENT '] = (bf.read_uint(2),bf.read_uint(2))
			elif tp == 4:
				#character_set_client collation_connection collation_server
				self.status_vars_dict['Q_CHARSET_CODE'] = (bf.read_uint(2),bf.read_uint(2),bf.read_uint(2))
			elif tp == 5:
				self.status_vars_dict['Q_TIME_ZONE_CODE'] = bf.read(bf.read_uint(1))
			elif tp == 6:
				self.status_vars_dict['Q_CATALOG_NZ_CODE'] = bf.read(bf.read_uint(1))
			elif tp == 7:
				self.status_vars_dict['Q_LC_TIME_NAMES_CODE'] = bf.read_uint(2)
			elif tp == 8:
				self.status_vars_dict['Q_CHARSET_DATABASE_CODE'] = bf.read_uint(2)
			elif tp == 9:
				self.status_vars_dict['Q_TABLE_MAP_FOR_UPDATE_CODE'] = bf.read_uint(8)
			elif tp == 10:
				continue
				self.status_vars_dict['Q_MASTER_DATA_WRITTEN_CODE'] = bf.read_uint(4)
			elif tp == 11:
				self.status_vars_dict['Q_INVOKER'] = {}
				self.status_vars_dict['Q_INVOKER']['user'] = bf.read(bf.read_uint(1)).decode()
				self.status_vars_dict['Q_INVOKER']['host'] = bf.read(bf.read_uint(1)).decode()
			elif tp == 12:
				mts_accessed_dbs = bf.read_uint(1)
				mts_dbs = []
				if mts_accessed_dbs > 16 : #MAX_DBS_IN_EVENT_MTS
					mts_accessed_dbs = 254 # OVER_MAX_DBS_IN_EVENT_MTS
				else:
					for x in range(mts_accessed_dbs):
						dbname = bf.read_string(b'\x00')
						if dbname == b'':
							break
						mts_dbs.append(dbname.decode())
				self.status_vars_dict['Q_UPDATED_DB_NAMES'] = mts_dbs
			elif tp == 13:
				self.status_vars_dict['Q_MICROSECONDS'] = bf.read_uint(3)
			elif tp == 14: #官方都没解析
				self.status_vars_dict['Q_COMMIT_TS'] = 0
			elif tp == 15:
				self.status_vars_dict['Q_COMMIT_TS2'] = 0
			elif tp == 16:
				self.status_vars_dict['Q_EXPLICIT_DEFAULTS_FOR_TIMESTAMP'] = bf.read_uint(1)
			elif tp == 17: #自带XID_EVENT
				self.status_vars_dict['Q_DDL_LOGGED_WITH_XID'] = bf.read_uint(8)
			elif tp == 18: # information_schema.COLLATION
				self.status_vars_dict['Q_DEFAULT_COLLATION_FOR_UTF8MB4'] = bf.read_uint(2)
			elif tp == 19: #
				self.status_vars_dict['Q_SQL_REQUIRE_PRIMARY_KEY'] = bf.read_uint(1)
			elif tp == 20:
				self.status_vars_dict['Q_DEFAULT_TABLE_ENCRYPTION'] = bf.read_uint(1)

		if self.verbose >= 3:
			self.debug(f"status_vars_dict : {self.status_vars_dict}")
