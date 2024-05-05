# write by ddcw @https://github.com/ddcw

from pymysqlbinlog.event import event
from pymysqlbinlog.event import event_header
from pymysqlbinlog.opt_metadata_type import *
import struct
import datetime
import time
import binascii
import base64
import sys
import json
from pymysqlbinlog.event_type import *
from pymysqlbinlog.filed_type import *
from pymysqlbinlog.mysql_json import jsonob

row_flags = {
1:"STMT_END_F",
2:"NO_FOREIGN_KEY_CHECKS_F",
4:"RELAXED_UNIQUE_CHECKS_F",
8:"COMPLETE_ROWS_F",
}
enum_extra_row_info_typecode = {
0:"NDB",
1:"PART"
}

class row_event(event):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)
		self.tablemap = kwargs['tablemap']
		self._event_bdata = kwargs['event_header']
		self.event_header = event_header(self._event_bdata) #记录event_type, 可能涉及到修改, 所以要传递过来方便点
		self.rollback = False # 标记是否要转为rollback
		self.checksum = False # 要不要checksum. 没啥用. 除了重新生成二进制文件
		self.row_offset = []  # 每行的起始offset(start,stop(update才有)), 方便rollback到base64的
		self.row = []

	def init(self,):
		# 设置rollback 相关信息

		# 初始化payload
		# @libbinlogevents/src/rows_event.cpp
		self.table_id = self.read_uint(6) # 5.1.4之前是4bytes
		self.flags = self.read_uint(2)
		#if self.verbose >= 3:
		#	self.debug(f"EVENT_TYPE:{self.event_header.event_type} TABLE_ID:{self.table_id}  flags:{self.flags} ({row_flags[self.flags]})")

		# extra data
		# @py-mysql-replication row_event.py
		self.extra_data_length = self.read_uint(2)
		if self.verbose >= 3:
			self.debug(f"var_header_len: {self.extra_data_length}")
		if self.extra_data_length > 2:
			self.extra_data_type = self.read_uint(1)
			if enum_extra_row_info_typecode[self.extra_data_type] == "NDB":
				self.ndb_info_length = self.read_uint(1)
				self.ndb_info_format= self.read_uint(1)
				self.ndb_info = self.read(self.ndb_info_length - 2)
				if self.verbose >= 3:
					self.debug(f"NDB INFO:",self.ndb_info)
			#partition
			elif enum_extra_row_info_typecode[self.extra_data_type] == "PART":
				if self.event_header.event_type in [ UPDATE_ROWS_EVENT,UPDATE_ROWS_EVENT_V1, PARTIAL_UPDATE_ROWS_EVENT ]:
					self.partition_id = self.read_uint(2)
					self.source_partition_id = self.read_uint(2)
					if self.verbose >= 3:
						self.debug(f"source_partition_id:{self.source_partition_id}")
				else:
					self.partition_id = self.read_uint(2)
				if self.verbose >= 3:
					self.debug(f"partition_id:{self.partition_id}")
			else:
				self.extra_data = self.read(self.extra_data_length-3)
				if self.verbose >= 3:
					self.debug(f"EXTRA DATA:",self.extra_data)


		# 官网的注释误人....
		self.width = self.read_pack_int()
		#self.cols = self.read_uint(int((self.width+7)/8)) #n_bits_len
		self.cols = (1<<64)-1 #虚假的繁华...
		if self.verbose >= 2:
			self.debug(f"WIDTH(字段数量):{self.width}  COLS(bitflag for used): {self.cols}")

		# before image
		self.before_image = self.read_uint(int((self.width+7)/8))

		# after image
		if self.event_header.event_type in [ UPDATE_ROWS_EVENT,UPDATE_ROWS_EVENT_V1, PARTIAL_UPDATE_ROWS_EVENT ]:
			self.after_image = self.read_uint(int((self.width+7)/8))
		else:
			self.after_image = self.before_image
		if self.verbose >= 3:
			self.debug(f"BEFORE_IMAGE:{self.before_image}  AFTER_IMAGE:{self.after_image}")
		# 剩下的就是读数据了.
		if self.verbose >= 3:
			self.debug(f"DATA SIZE: {len(self.bdata)-self.offset}")
			self.debug(f"{self.bdata[self.offset:]}")
		#nullbits = self.read_uint(int((self.width+7)/8))
		self.row_offset = []
		self.row = []
		self.metadata = []
		while True:
			start_offset,stop_offset,data,metadata = self._read_row(self.cols)
			if start_offset == -1:
				break
			if self.event_header.event_type in [WRITE_ROWS_EVENT_V1, WRITE_ROWS_EVENT,]: #insert
				self.row_offset.append([start_offset,stop_offset])
				self.row.append(data)
				self.metadata.append(metadata)
			elif self.event_header.event_type in [DELETE_ROWS_EVENT_V1,DELETE_ROWS_EVENT,]: #delete
				self.row_offset.append([start_offset,stop_offset])
				self.row.append(data)
				self.metadata.append(metadata)
			elif self.event_header.event_type in [UPDATE_ROWS_EVENT_V1,UPDATE_ROWS_EVENT,PARTIAL_UPDATE_ROWS_EVENT]: #update
				start_offset2,stop_offset2,data2,metadata2 = self._read_row(self.cols)
				self.row_offset.append([start_offset,stop_offset,start_offset2,stop_offset2])
				self.row.append([data,data2])
				self.metadata.append([metadata,metadata2])
		#self.debug(self.row)
		#self.rollback = True
		#print(self.read_b64())
		self.checksum = True
		#sys.stdout.write(self.read_b64())
		#for x in self.read_sql():
		#	print(f"{x}")

	def _read_unsigned_flag(self,):
		return False # 符号信息不准... 还是全部当作有符号吧...
		if self.tablemap.signed_list_boolean:
			self._signedno += 1
			return self.tablemap.signed_list[self._signedno-1]
		else:
			return True

	def _read_metadata(self,n):
		bdata = self.tablemap.metadata[self._metadta_offset:self._metadta_offset+n]
		self._metadta_offset += n
		return bdata
			
	def read_geom_type(self,):
		self._geomno +=1
		return self.tablemap.geom_type[self._geomno]

	def _read_row(self,cols):
		# 读一行数据, 返回 start_offset, stop_offset, data
		start_offset = self.offset
		nullbits_data = self.read(int((self.width+7)/8))
		if nullbits_data == b'':
			return -1,-1,[],[]
		nullbits = int.from_bytes(nullbits_data,'little',signed=False)
		row = []
		if self.verbose >= 3:
			self.debug(f"nullbits:{nullbits}")
		coln = -1
		self._geomno = -1
		self._signedno = 0
		self._metadta_offset = 0
		metadata = []
		for col_type in self.tablemap.column_type:
			if self.verbose >= 3:
				self.debug(f"READ TYPE FOR: {col_type}")
			coln += 1
			mdata = 0
			#判断是否被使用
			if not (cols&(1<<coln)):
				row.append(None)
			#null判断 TODO
			if self.tablemap.null_bits_boolean[coln] and (nullbits&(1<<coln)):
				row.append(None)
			if col_type == MYSQL_TYPE_NEWDECIMAL: # 没见过MYSQL_TYPE_DECIMAL
				#t00 = self.read_uint(1)
				#t0  = self.read_uint(1)
				#t11 = int(t00/9)*4 + int(((t00%2)+1)/2)
				#t1  = int(t0/9)*4 + int(((t0%2)+1)/2)
				# 还是得参考ibd2sql  懒得管名字了. 能解析就行(看得我头皮发麻)...
				#self.debug(self.bdata[self.offset:])
				total_digits = struct.unpack('<B',self._read_metadata(1))[0]
				decimal_digits  = struct.unpack('<B',self._read_metadata(1))[0]
				integer_p1_count = int((total_digits - decimal_digits)/9) #
				integer_p2_count = total_digits - decimal_digits - integer_p1_count*9
				integer_size = integer_p1_count*4 + int((integer_p2_count+1)/2)
				decimal_p1_count = int(decimal_digits/9)
				decimal_p2_count = decimal_digits - decimal_p1_count*9
				decimal_size = decimal_p1_count*4 + int((decimal_p2_count+1)/2)
				total_size = integer_size + decimal_size
				#self.debug(total_size,self.tablemap.metadata)
				bdata = self.read(total_size)
				p1 = integer_size
				p2 = decimal_size
				p1_bdata = bdata[:p1]
				p2_bdata = bdata[p1:]
				p1_data = int.from_bytes(p1_bdata,'big',signed=True)
				p2_data = int.from_bytes(p2_bdata,'big',signed=True)
				p1_n = (p1*8)-1
				p2_n = (p2*8)-1
				if p1_data < 0:
					p1_data = p1_data + (2**(8*p1-1))
				else:
					p1_data = p1_data - (2**(8*p1-1)) + 1
				if p2_data < 0:
					p2_data = -(p2_data + 1)
				data = f"{p1_data}.{p2_data}"
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* DECIMAL meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_TINY:
				data = self.read_uint(1) if self._read_unsigned_flag() else self.read_int(1)
				self.debug(f"TINY INT(1): {data}")
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* TINY meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_SHORT:
				data = self.read_uint(2) if self._read_unsigned_flag() else self.read_int(2)
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* SHORT meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_LONG:
				data = self.read_uint(4) if self._read_unsigned_flag() else self.read_int(4)
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* LONG meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_FLOAT:
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				data = struct.unpack('f',self.read(4))[0]
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* FLOAT meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_DOUBLE:
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				data = struct.unpack('d',self.read(8))[0]
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* DOUBLE meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_TIMESTAMP:
				ltime = time.localtime(self.read_uint(4))
				data = f"{ltime.tm_year}-{ltime.tm_mon}-{ltime.tm_mday} {ltime.tm_hour}:{ltime.tm_min}:{ltime.tm_sec}"
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={ltime} /* TIMESTAMP meta=0 is_null=0*/")
			elif col_type == MYSQL_TYPE_LONGLONG:
				data = self.read_uint(8) if self._read_unsigned_flag() else self.read_int(8)
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* LONGLONG meta=0 is_null=0*/")
			elif col_type == MYSQL_TYPE_INT24:
				data = self.read_uint(3) if self._read_unsigned_flag() else self.read_int(3)
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* INT24 meta=0 is_null=0*/")
			elif col_type == MYSQL_TYPE_DATE:
				tim = self.read_uint(3)
				year = (tim & ((1 << 15) - 1) << 9) >> 9
				month = (tim & ((1 << 4) - 1) << 5) >> 5
				day = (tim & ((1 << 5) - 1))
				dt = datetime.date(year=year,month=month,day=day)
				data = f'{year}-{month}-{day}'
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={year}-{month}-{day} /* DATE meta=0 is_null=0*/")
			elif col_type == MYSQL_TYPE_TIME:
				tim = self.read_uint(3)
				dt = datetime.timedelta(hours=int(tim / 10000), minutes=int((tim % 10000) / 100), seconds=int(tim % 100)) #@pymysqlreplication
				data = dt
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={dt} /* TIME meta=0 is_null=0*/")
			elif col_type == MYSQL_TYPE_TIME2:
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				bdata = self.read(3+int((mdata+1)/2))
				idata = int.from_bytes(bdata[:3],'big')
				hour = ((idata & ((1 << 10) - 1) << 12) >> 12)
				minute = (idata & ((1 << 6) - 1) << 6) >> 6
				second = (idata& ((1 << 6) - 1))
				great0 = True if idata&(1<<23) else False
				fraction = int.from_bytes(bdata[3:],'big') if len(bdata)>3 else None
				if fraction is None:
					dt = f'{hour}:{minute}:{second}' if great0 else f'-{hour}:{minute}:{second}'
				else:
					dt = f'{hour}:{minute}:{second}.{fraction}' if great0 else f'-{hour}:{minute}:{second}.{fraction}'
				data = dt
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={dt} /* TIME2 meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_DATETIME:
				pass
			elif col_type == MYSQL_TYPE_DATETIME2:
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				bdata = self.read(5+int((mdata+1)/2))
				idata = int.from_bytes(bdata[:5],'big')
				year_month = ((idata & ((1 << 17) - 1) << 22) >> 22)
				year = int(year_month/13)
				month = int(year_month%13)
				day = ((idata & ((1 << 5) - 1) << 17) >> 17)
				hour = ((idata & ((1 << 5) - 1) << 12) >> 12)
				minute = ((idata & ((1 << 6) - 1) << 6) >> 6)
				second = (idata& ((1 << 6) - 1))
				great0 = True if idata&(1<<39) else False
				fraction = int.from_bytes(bdata[5:],'big') if len(bdata)>5 else None
				if fraction is None:
					dt = f'{year}-{month}-{day} {hour}:{minute}:{second}' if great0 else f'-{year}-{month}-{day} {hour}:{minute}:{second}'
				else:
					dt = f'{year}-{month}-{day} {hour}:{minute}:{second}.{fraction}' if great0 else f'-{year}-{month}-{day} {hour}:{minute}:{second}.{fraction}'
				data = dt
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={dt} /* DATETIME2 meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_TIMESTAMP2:
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				bdata = self.read(4+int((mdata+1)/2))
				ltime = time.localtime(int.from_bytes(bdata[:4],'big'))
				fraction = int.from_bytes(bdata[4:],'big') if len(bdata)>4 else None
				dt = f'{ltime.tm_year}-{ltime.tm_mon}-{ltime.tm_mday} {ltime.tm_hour}:{ltime.tm_min}:{ltime.tm_sec}{"."+fraction if fraction is not None else ""}'
				data = dt
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={dt} /* TIMESTAMP2  meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_YEAR:
				data = self.read_uint(1) + 1900
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* YEAR meta=null is_null=0*/")
			elif col_type == MYSQL_TYPE_STRING: 
				mdata = struct.unpack('>H',self._read_metadata(2))[0]
				mtype = mdata >>8
				#msize = (mdata >> 8)&mdata
			#	if (mdata >> 8) == 254: #binary
			#		msize = (mdata &((1<<8)-1))
			#		data = hex(int.from_bytes(self.read(self.read_uint(1)),'big'))
				if mtype == 247: #enum
					msize = (mdata &((1<<8)-1))
					data = self.read_uint(2 if msize >= 2**8 else 1)
				elif mtype == 248: #set
					msize = (mdata &((1<<8)-1))
					#self.debug(msize)
					data = self.read_uint(int((msize+7)/8))
				else:
					# @libbinlogevents/src/binary_log_funcs.cpp
					mmaxsize = (((mdata >> 4) & 0x300) ^ 0x300) + (mdata& 0x00ff)
					if mmaxsize > 255:
						msize = self.read_uint(2)
					else:
						msize = self.read_uint(1)
					data = self.read(msize).decode()
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* STRING meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_VARCHAR: #varchar/varbinary
				mdata = struct.unpack('<H',self._read_metadata(2))[0]
				msize = self.read_uint(2) if mdata > 255 else self.read_uint(1)
				bdata = self.read(msize)
				try:
					data = bdata.decode()
				except:
					data = bdata
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* VARCHAR meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_BIT: #bit
				mdata = struct.unpack('<H',self._read_metadata(2))[0]
				msize = int((mdata+7)/8)
				data = self.read_uint(msize)
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* BIT meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_JSON: #JSON
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				msize = self.read_uint(mdata)
				_tdata = self.read(msize)
				data = jsonob(_tdata[1:],int.from_bytes(_tdata[:1],'little')).init()
				data = json.dumps(data)
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* JSON meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_BLOB: #BLOB/TEXT ...
				"""4:longblob/longtext"""
				"""3:mediumblob/mediumtext"""
				"""2:blob/text"""
				"""1:tinyblob/tinytext"""
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				data = self.read(self.read_uint(mdata)).decode()
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* BIG TYPE meta={mdata} is_null=0*/")
			elif col_type == MYSQL_TYPE_GEOMETRY: #坐标之力
				mdata = struct.unpack('<B',self._read_metadata(1))[0]
				msize = self.read_uint(mdata)
				data1 = self.read(msize).hex()
				data = f"0x{data1.upper()}"
				row.append(data)
				if self.verbose >= 1:
					self.debug(f"### @{coln+1}={data} /* GEOMETRY meta={mdata} is_null=0*/")
			metadata.append(mdata)

		stop_offset = self.offset
		if self.verbose >= 1:
			self.debug(f"ROW:{row}")
		return start_offset,stop_offset,row,metadata

	def read_sql(self,):
		"""拼接为SQL"""
		sqll = []
		for v,m in zip(self.row,self.metadata):
			if (self.event_header.event_type == WRITE_ROWS_EVENT and not self.rollback) or (self.rollback and self.event_header.event_type == DELETE_ROWS_EVENT):
				v1 = self.tablemap.read_column_value(v,m,1)
				sql = f"INSERT INTO `{self.tablemap.dbname}`.`{self.tablemap.table_name}` values({v1})"	
			elif (self.event_header.event_type == DELETE_ROWS_EVENT and not self.rollback) or (self.rollback and self.event_header.event_type == WRITE_ROWS_EVENT):
				v2 = self.tablemap.read_column_value(v,m,2)
				sql = f"DELETE FROM `{self.tablemap.dbname}`.`{self.tablemap.table_name}` where {v2}"
			elif self.event_header.event_type in [UPDATE_ROWS_EVENT,PARTIAL_UPDATE_ROWS_EVENT]:
				if self.rollback:
					v1 = self.tablemap.read_column_value(v[0],m[0])
					v2 = self.tablemap.read_column_value(v[1],m[1],2)
					sql = f"UPDATE `{self.tablemap.dbname}`.`{self.tablemap.table_name}` set {v1} where {v2}"
				else:
					v1 = self.tablemap.read_column_value(v[0],m[0],2)
					v2 = self.tablemap.read_column_value(v[1],m[1])
					sql = f"UPDATE `{self.tablemap.dbname}`.`{self.tablemap.table_name}` set {v2} where {v1}"
			sqll.append(f"{sql}")
					
		return sqll

	def read_bdata(self,):
		"""返回二进制数据格式"""
		if self.rollback:
			if self.event_header.event_type == WRITE_ROWS_EVENT:
				bdata_header = self.event_header.get_bdata(DELETE_ROWS_EVENT)
			elif self.event_header.event_type == DELETE_ROWS_EVENT:
				bdata_header = self.event_header.get_bdata(WRITE_ROWS_EVENT)
			if self.event_header.event_type in [UPDATE_ROWS_EVENT,PARTIAL_UPDATE_ROWS_EVENT]:
				bdata_header = self.event_header.get_bdata()
				bdata = self.bdata[:self.row_offset[0][0]]
				for x in self.row_offset:
					bdata += self.bdata[x[2]:x[3]]
					bdata += self.bdata[x[0]:x[1]]
			else:
				bdata = self.bdata
		else:
			bdata_header = self.event_header.get_bdata()
			bdata = self.bdata
		mbdata = bdata_header + bdata
		tmbdata = self.tablemap.abdata
		if self.checksum:
			mbdata += struct.pack('<L',binascii.crc32(mbdata))
			tmbdata += struct.pack('<L',binascii.crc32(tmbdata))
		return tmbdata,mbdata
	def read_b64(self,):
		"""返回base64数据"""
		tmbdata,mbdata = self.read_bdata()
		rdata = ''
		#table_map
		for x in range(0,len(tmbdata),57):
			rdata += base64.b64encode(tmbdata[x:x+57]).decode() + "\n"
		#row_event
		for x in range(0,len(mbdata),57):
			rdata += base64.b64encode(mbdata[x:x+57]).decode() + "\n"
		return f"\nBINLOG '\n{rdata}'"


class tablemap_event(event):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)
		# binlog里面没记录如下信息, 得从数据库里面捞(如果要生成SQL的话).
		self.column_name = [] #字段名字
		self.signed_list = [] #符号. 只对number类型有用, 所以和字段长度大概率是不一样多的.
		self.signed_list_boolean = False #表示是否有读取到符号信息
		self._event_bdata = kwargs['event_header']
		self.event_header = event_header(self._event_bdata) #记录event_type, 可能涉及到修改, 所以要传递过来方便点
		self.abdata = self._event_bdata + self.bdata
		#self.init()
	def init(self,):
		self.table_id = self.read_uint(6)
		self.flags = self.read_uint(2)
		dbname_length = self.read_uint(1)
		#self.debug(f"DBNAME LENGTH:{dbname_length}")
		self.dbname = self.read(dbname_length).decode()
		_ = self.read(1) #\x00结尾
		tablename_length = self.read_uint(1)
		#self.debug(f"TABLENAME LENGTH:{tablename_length}")
		self.table_name = self.read(tablename_length).decode()
		_ = self.read(1)
		self.column_count = self.read_pack_int()
		self.column_type = [ x for x in self.read(self.column_count) ]
		self.metadata_length = self.read_pack_int()
		self.metadata = self.read(self.metadata_length) # 长度,精度之类的
		self.null_bits = self.read_uint(int((self.column_count+7)/8))
		null_bits = [ 1 if self.null_bits&(1<<x) else 0 for x in range(8*int((self.column_count+7)/8)) ]
		self.null_bits_list = null_bits
		# True: 可以为空, False:不能为空. 约束
		self.null_bits_boolean = [ True if x == 1 else False for x in null_bits ][:self.column_count]
		opt = []
		if self.verbose >= 3:
			self.debug(f"offset:{self.offset} size:{len(self.bdata)}")
		# 8.0 新增opt optional metadata fields (比如是否有符号等信息.)
		while True:
			t = self.read_uint(1)
			if self._bdata == b'':
				break
			l = self.read_pack_int()
			v = self.read(l)
			opt.append((t,l,v))
		self.opt = opt
		if self.verbose >= 3:
			self.debug(f"offset:{self.offset} size:{len(self.bdata)}")
			self.debug(f"TABLE_ID       : {self.table_id}")
			self.debug(f"FLAGS          : {self.flags}")
			self.debug(f"DBNAME         : {self.dbname}")
			self.debug(f"TABLENAME      : {self.table_name}")
			self.debug(f"COLUMN COUNT   : {self.column_count}")
			self.debug(f"column type    : {self.column_type}") #是MYSQL_TYPE_ 非innodb类型
			self.debug(f"metadata length: {self.metadata_length}")
			self.debug(f"metadata       : {self.metadata}")
			self.debug(f"null_bits      : {self.null_bits}")
			self.debug(f"null_bits_list : {self.null_bits_list}")
			self.debug(f"null_bit_bool  : {self.null_bits_boolean}")
			self.debug(f"opt            : {self.opt}")

		# 符号处理.
		if len(self.opt) > 0: # 8.0, 有opt
			for x in self.opt:
				if x[0] == IGNEDNESS:
					v = int.from_bytes(x[2],'little',signed=False)
					self.signed_list = [ True if v&(1<<y) else False for y in range(len(x[2])*8) ]
					self.signed_list.reverse()
					self.signed_list_boolean = True
					if self.verbose >= 3:
						self.debug(f"signed_list(True:unsigned) {self.signed_list}")
				elif x[0] == COLUMN_NAME:
					self.column_name = []
					offset  = 0
					while True:
						if offset >= x[1]:
							break
						namesize = struct.unpack('<B',x[2][offset:offset+1])[0]
						offset += 1
						column = x[2][offset:offset+namesize].decode()
						offset += namesize
						self.column_name.append(column)
					if self.verbose >= 3:
						self.debug(f"COLUMN NAME: {self.column_name}")
				elif x[0] == GEOMETRY_TYPE: #只是空间坐标的类型
					self.geom_type = [ i for i in x[2] ]

		# 元数据处理, 都差点生成DDL了, 还差默认值,自增,分区之类的信息了.
		column = {}
		signed_n = 0
		for x in range(1,self.column_count+1):
			# 这里的int 代表number, 主要是方便读信息而已
			column[x] = {'name':'id', 'type':'int', 'unsigned':True, 'size':'xx','isvar':True, 'nullable':True, 'opt':''}
		self.column = column

	def read_column_name(self,):
		"""如果没设置字段名字, 就返回@n"""
		if len(self.column_name) == 0:
			return  [ f"@{x}" for x in range(1,self.column_count+1) ]
		else:
			return self.column_name

	def read_column_value(self,vl,mt,kvork=0):
		cl = self.read_column_name()
		rdata = ""
		if len(vl) == 0:
			return rdata
		for x in range(self.column_count):
			t = self.column_type[x]
			k = cl[x]
			v = vl[x]
			m = mt[x]
			if (m >> 8) in (247,248) or t in [MYSQL_TYPE_DECIMAL, MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT,MYSQL_TYPE_LONG,MYSQL_TYPE_FLOAT,MYSQL_TYPE_DOUBLE,MYSQL_TYPE_LONGLONG,MYSQL_TYPE_INT24,MYSQL_TYPE_YEAR,MYSQL_TYPE_NEWDECIMAL,MYSQL_TYPE_ENUM,MYSQL_TYPE_SET,MYSQL_TYPE_BIT,MYSQL_TYPE_GEOMETRY]:
				if kvork == 0:
					rdata += f"`{k}`={v}, " if v is not None else f"`{k}`=null, " 
				elif kvork == 1:
					rdata += f"{v}, " if v is not None else f"null, " 
				elif kvork == 2:
					rdata += f"`{k}`={v} and " if v is not None else f"`{k}`=null and "
			else:
				if kvork == 0:
					rdata += f"`{k}`='{v}', "  if v is not None else f"`{k}`=null, "
				elif kvork == 1:
					rdata += f"'{v}', " if v is not None else f"null, "
				elif kvork == 2:
					rdata += f"`{k}`='{v}' and " if v is not None else f"`{k}`=null and "
		return rdata[:-2] if kvork < 2 else rdata[:-5]

