#!/usr/bin/python
import redis
import json
import time
import logging
import pdb
import time
import uuid
from psycopg2 import pool
from multiprocessing import Process, Lock
from config import *
import sys
from datetime import datetime
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')
'''this class is read data by redis and storage the data to the psql
mutiprocess action'''
class SynData():
	def __init__(self,process_number=2):
		self.process_number=process_number;
		self.lock = Lock()
	"""storage the date to psql"""
	def storage_data(self,lock,i):
		logging.basicConfig(filename='msg_record.log',level=logging.ERROR)
		conn_pool=pool.SimpleConnectionPool(1,10,host=psql_host,dbname=psql_db,user=psql_user,password=psql_pass)
		redis_connection=redis.StrictRedis(host=redis_host, port=redis_port, db=0)
		while 1:
			time.sleep(1)
			key=str(uuid.uuid1())
			redis_data=self.get_data(redis_connection)
			if not redis_data or len(redis_data)==0:
				continue
			psql_connection =conn_pool.getconn(key)
			for item in redis_data:
				if not item :
					continue
				try:
					data=json.loads(item)
					self.add_msg_record(psql_connection,data)
					self.update_time(redis_connection,data,item)
						
				except Exception as e:
					date=datetime.now()
					current_time=date.strftime('%Y-%m-%d %H:%M:%S')
					logging.error("%s-***-%s**--",item,e)
			
			if psql_connection:
				conn_pool.putconn(psql_connection,key)

		
	def add_msg_record(self,conn,data):
		cur=conn.cursor()
		#pdb.set_trace()
		platform=1
		sn=""
		type=data["type"]
		if "platform" in data:
			platform=data["platform"]
		if "sn" in data:
			sn=data["sn"]
		sql="""insert into msg_record(msg_uuid,msg_type,msg_content,msg_from,msg_from_type,msg_to,msg_to_type,
			 msg_send_time,msg_receive_time,msg_status,msg_paltform,msg_sn) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""";
		content=json.dumps(data["data"]).decode('unicode-escape').encode('utf8')
		cur.execute(sql,(
					data["id"],
					data["type"]
					,content,
					data["from"]["id"],
					data["from"]["type"]
					,data["to"]["id"],
					data["to"]["type"],
					data["time"],
					data["receiveTime"],
					1,
					platform,
					sn
					))
		
			
		conn.commit()

	"""get data from redis special key"""

	def get_data(self,redis_connection):
		redis_data=[]
		#self.lock.acquire()
		try:
			llen=redis_connection.llen(redis_key);
			if llen>0:
				redis_data=redis_connection.lrange(redis_key,0,llen-1)
				redis_connection.ltrim(redis_key,llen,-1)
		except Exception as e:
			logging.error("redis_error***-%s",e)
		#self.lock.release()
		return redis_data
		
			
	"""create process """

	def create_process(self):
		for num in range(self.process_number):
			Process(target=self.storage_data, args=(self.lock,num)).start()
		
	def update_time(self,redis_client,data,item):
		key=data["to"]["id"]+"_LAST_DATA"
		redis_client.hset(key,data["from"]["id"],item)	
		
		print item
		key=data["from"]["id"]+"_LAST_DATA"
		redis_client.hset(key,data["to"]["id"],item)	
	
		
			
if __name__=="__main__":
	syn=SynData()
	#syn.create_process()
	syn.storage_data(1,2)
