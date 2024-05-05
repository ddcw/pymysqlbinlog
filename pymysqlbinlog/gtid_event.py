# write by ddcw @https://github.com/ddcw

from pymysqlbinlog.event import event
import uuid


# @libbinlogevents/include/control_events.h
# @libbinlogevents/src/control_events.cpp
LOGICAL_TIMESTAMP_TYPECODE = 2
ENCODED_COMMIT_TIMESTAMP_LENGTH = 55
ENCODED_SERVER_VERSION_LENGTH = 31

class gtid_event(event):
	def __init__(self,*args,**kwargs):
		"""
----------------------------------------------------------------------------------
|  对象         |              大小           |               描述               |
----------------------------------------------------------------------------------
   GTID_FLAGS                  1 
   SID                         16                            server uuid
   GNO                         8                             gtid
   lt_type                     1                             logical timestamp
   last_committed              8 if lt_type == 2 else 0
   sequence_number             8 if lt_type == 2 else 0
   immediate_commit_timestamp  8
   original_commit_timestamp   0/8
   transaction_length          1-9
   immediate_server_version    4
   original_server_version     0/4
----------------------------------------------------------------------------------
		"""
		super().__init__(*args,**kwargs)
		self.IS_ANON_GTID = False
		

	def init(self,):
		self.offset = 0
		self.GTID_FLAGS = self.read_uint(1)
		self.debug(f"GTID_FLAGS:{self.GTID_FLAGS}")
		self.SID = str(uuid.UUID(bytes=self.read(16)))
		self.debug(f"SERVER_UUID:{self.SID}")
		self.GNO = self.read_uint(8)
		self.debug(f"GTID:{self.GNO}")
		self.lt_type = self.read_uint(1) #The type of logical timestamp used in the logical clock fields.
		self.debug(f"lt_type:{self.lt_type} (logical timestamp)") #  always equal to LOGICAL_TIMESTAMP_TYPECODE
		_ = """
		if (lc_typecode == LOGICAL_TIMESTAMP_TYPECODE) {
      READER_TRY_SET(last_committed, read<uint64_t>);
      READER_TRY_SET(sequence_number, read<uint64_t>);
		"""
		if self.lt_type == LOGICAL_TIMESTAMP_TYPECODE:
			self.last_committed = self.read_uint(8)
			self.debug(f"last_committed:{self.last_committed}")
			self.sequence_number = self.read_uint(8)
			self.debug(f"sequence_number:{self.sequence_number}")
		else:
			self.last_committed = None
			self.sequence_number = None

		## 下面部分 对于匿名事务是不存在的
		self.immediate_commit_timestamp = self.read_uint(7)
		if (self.immediate_commit_timestamp & (1<<ENCODED_COMMIT_TIMESTAMP_LENGTH))!=0:
			#self.immediate_commit_timestamp = self.immediate_commit_timestamp - (1<<ENCODED_COMMIT_TIMESTAMP_LENGTH)
			self.immediate_commit_timestamp &= ~(1<<ENCODED_COMMIT_TIMESTAMP_LENGTH)
			self.original_commit_timestamp  = self.read_uint(7)
		else:
			self.original_commit_timestamp = self.immediate_commit_timestamp
		self.debug(f"immediate_commit_timestamp:{self.immediate_commit_timestamp}")
		self.debug(f"original_commit_timestamp:{self.original_commit_timestamp}")

		self.transaction_length = self.read_net_int()
		self.debug(f"transaction_length:{self.transaction_length}")
		self.immediate_server_version = self.read_uint(4)
		if (self.immediate_server_version & (1<<ENCODED_SERVER_VERSION_LENGTH)) != 0:
			self.immediate_server_version &= ~(1<<ENCODED_SERVER_VERSION_LENGTH)
			self.original_server_version = self.read_uint(4)
		else:
			self.original_server_version = self.immediate_server_version
		self.debug(f"immediate_server_version:{self.immediate_server_version}")
		self.debug(f"original_server_version:{self.original_server_version}")
		#print(len(self.bdata),self.offset)

class pre_gtid_event(gtid_event):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)
		"""
------------------------------------------
gtid_number	8 bytes
gtid_list	gtid_number*gtid_info
		gtid_info:
			server_uuid
			group_gno_number  8 bytes (多少对连续的gno)
				start_gno 8 bytes
				stop_gno  8 bytes
				"""

	def init(self):
		self.offset = 0
		gtid_number = self.read_uint(8)
		gtid_list = []
		for x in range(gtid_number):
			server_uid_bdata = self.read(16)
			if server_uid_bdata == b'':
				break
			sid = str(uuid.UUID(bytes=server_uid_bdata))
			group_gno_number = self.read_uint(8)
			gtid_info = sid
			for y in range(group_gno_number):
				start = self.read_uint(8)
				stop  = self.read_uint(8)-1
				gtid_info += f":{start}-{stop}"
			gtid_list.append(gtid_info)
		if self.verbose >= 2:
			self.debug(f"Previous-GTIDs: {gtid_list}")

# 匿名gtid
class anonymous_gtid_event(gtid_event):
	def __init__(self,*args,**kwargs):
		super().__init__(*args,**kwargs)
		self.IS_ANON_GTID = True
