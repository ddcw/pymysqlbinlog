#write by ddcw @github.com/ddcw

import struct

class event(object):
	def __init__(self,*args,**kwargs):
		self.offset = 0
		self.bdata = kwargs['bdata']
		self._bdata = b''
		self.debug2 = kwargs['debug'] if 'debug' in kwargs else None
		self.verbose = kwargs['verbose'] if 'verbose' in kwargs else 0

	def read_uint(self,n):
		return int.from_bytes(self.read(n),'little',signed=False)

	def read_int(self,n):
		return int.from_bytes(self.read(n),'little',signed=True)

	def read(self,n):
		offset1 = self.offset
		data = self.bdata[self.offset:self.offset+n]
		self._bdata = data
		if len(data) != n:
			return b''
		n = 0 if data == b"" else n
		self.offset += n
		offset2 = self.offset
		if self.verbose >=3:
			self.debug(f"READ DATA FROM EVENT PAYLOAD OFFSET:{offset1} ---> {offset2} SIZE:{n} data:{data}")
		return data

	def read_pack_int(self):
		"""#https://dev.mysql.com/doc/dev/mysql-server/latest/classmysql_1_1binlog_1_1event_1_1Binary__log__event.html#packed_integer
---------------------------------------------------------------------------------------------------------
First byte   format
0-250        The first byte is the number (in the range 0-250), and no more bytes are used.
252          Two more bytes are used.   The number is in the range 251-0xffff.
253          Three more bytes are used. The number is in the range 0xffff-0xffffff.
254          Eight more bytes are used. The number is in the range 0xffffff-0xffffffffffffffff.
---------------------------------------------------------------------------------------------------------
		"""
		return self.read_net_int()
		fb = self.read_uint(1)
		bdata = self._bdata
		rdata = 0
		if fb <= 250:
			rdata = fb
		elif fb == 252:
			rdata = int.from_bytes(bdata+self.read(1),'little',signed=False)
		elif fb == 253:
			rdata = int.from_bytes(bdata+self.read(2),'little',signed=False)
		elif fb == 254:
			rdata = int.from_bytes(bdata+self.read(7),'little',signed=False)
		return rdata
	# @mysys/pack.cc net_field_length_size
	def read_net_int(self,):
		"""
		1 3 4 9 (不含第一字节)
		"""
		data = self.read_uint(1)
		if data < 251:
			return data
		elif data == 251:
			return self.read_uint(1)
		elif data == 252:
			return self.read_uint(2)
		elif data == 253:
			return self.read_uint(3)
		else:
			return self.read_uint(8)

	def debug(self,*args):
		if self.debug2 is not None:
			self.debug2(*args)

class event_header():
	def __init__(self,bdata):
		"""
# https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html
# libbinlogevents/src/binlog_event.cpp
---------------------- ---------------------------------------------------
|  timestamp     |     4 bytes    |    seconds since unix epoch          |
|  event_type    |     1 byte     |    event类型                         |
|  server_id     |     4 bytes    |    执行这个event的server_id          |
|  event_size    |     4 bytes    |    这个event大小(含event_header)     |
|  log_pos       |     4 bytes    |    距离下一个event的位置             |
|  flags         |     2 bytes    |    flags                             |
---------------------- ---------------------------------------------------
        """
		self.bdata = bdata
		self.timestamp, self.event_type, self.server_id, self.event_size, self.log_pos, self.flags = struct.unpack("<LBLLLH",bdata)
	def get_bdata(self,event_type=None):
		"""
		转为二进制类型
		"""
		if event_type is None:
			event_type = self.event_type
		bdata = struct.pack('<LBLLLH',self.timestamp, event_type, self.server_id, self.event_size, self.log_pos, self.flags)
		return bdata

	def __str__(self):
		return f"timestamp:{self.timestamp}  event_type:{self.event_type}  server_id:{self.server_id}  event_size:{self.event_size}  log_pos:{self.log_pos}  flags:{self.flags}"
		
