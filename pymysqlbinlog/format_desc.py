# write by ddcw @https://github.com/ddcw
from pymysqlbinlog.event import event
import struct
#import binascii

EVENT_LEN_OFFSET = 9

class format_desc_event(event):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)

	def init(self):
		self.offset = 0
		self.checksum = False
		self.binlog_version = self.read_uint(2)
		self.debug(f"BINLOG VERSION:{self.binlog_version}")
		self.mysql_version = self.read(50).decode()
		self.debug(f"SERVER VERSION:{self.mysql_version}")
		self.create_timestamp = self.read_uint(4)
		self.debug(f"create_timestamp:{self.create_timestamp}")
		self.event_header_length = self.read_uint(1)
		self.debug(f"event_header_length:{self.event_header_length} (equal 19)")
		if self.mysql_version[:1] == "5":
			self.event_post_header_len = self.read(38)
		elif self.mysql_version[:4] == "8.4.":
			self.event_post_header_len = self.read(43) # FOR MYSQL 8.4
		elif self.mysql_version[:1] == "8":
			self.event_post_header_len = self.read(41)
		self.debug(f"event_post_header_len: {[ x for x in self.event_post_header_len ]}")
		#@libbinlogevents/include/binlog_event.h
		self.checksum_alg = self.read_uint(1) #结尾的校验值算法, 也可能没得
		self.debug(f"checksum algorithm: {self.checksum_alg} (0:off 1:crc32)")
		if self.checksum_alg:
			self.debug(f"checksum value: {hex(self.read_uint(4))} ")
			self.checksum = True
		
