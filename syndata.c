#include "common.h"
#include <unistd.h>
redisContext * context;
static void  writeDataToPgsql(data_result * record, PGconn * con);
/*
get data from redis the data format is json
PGconn *conn
read chat data from redis data type is list */
void destory(data_result * result);
static void  getDataFromRedis(PGconn * con){
	json_object * resultArray=NULL;
	static char * redisKey = "IM:allmessagequeue";
	redisReply * redisResult=NULL;
	/*if(context!=NULL&&context->err){
		exit(1)
	}*/
	if(context!=NULL&&!context->err){
		redisResult  = redisCommand(context,"llen %s",redisKey);
		int llen=redisResult->integer;
		if(llen==0){
			printf("redis is empty");
			free(redisResult);
			return;
		}

		printf("%d\r\n",llen);
		free(redisResult);
		redisResult = redisCommand(context,"LRANGE %s %d %d ",redisKey,0,llen-1);
		if(redisResult->type==REDIS_REPLY_ARRAY)
		{
			data_result *  chatResult = malloc(sizeof(data_result));
			chatResult->records = malloc(sizeof(msg_record)*redisResult->elements);
			chatResult->msg_count=redisResult->elements;
			resultArray=json_object_new_array();
			const char * recordContent=NULL;
			int j;
			msg_record *  records=chatResult->records;	
			for (j = 0; j < redisResult->elements; j++) {
				recordContent=redisResult->element[j]->str;				
				json_object * record=json_tokener_parse(recordContent);
				json_object_array_add(resultArray,record);
				json_object * msguuid,* msgtype,* msgcontent,* msgfrom;
				json_object * msgfromtype, * msgto , *msgtotype,* msgsendtime;
				json_object * msgreceivetime,*msgstatus,*msgplatform,* msgsn;
				json_object_object_get_ex(record,"type",&msgtype);
				records[j].msg_type=json_object_get_string(msgtype);
				if(strcmp(records[j].msg_type,"offLine")==0){
					continue;
				}
				json_object_object_get_ex(record,"id",&msguuid);
				records[j].msg_uuid=json_object_get_string(msguuid);

				

				json_object_object_get_ex(record,"data",&msgcontent);
				records[j].msg_content=json_object_get_string(msgcontent);		
			

				json_object_object_get_ex(record,"from",&msgfrom);
				json_object * from_uid;
				json_object_object_get_ex(msgfrom,"id",&from_uid);
				
				records[j].msg_from=json_object_get_string(from_uid);

				json_object_object_get_ex(msgfrom,"type",&msgfromtype);
				records[j].msg_from_type=json_object_get_int(msgfromtype);

				json_object_object_get_ex(record,"to",&msgto);
				json_object * to_uid;
				json_object_object_get_ex(msgto,"id",&to_uid);
				records[j].msg_to=json_object_get_string(to_uid);

				
				json_object_object_get_ex(msgto,"type",&msgtotype);
				records[j].msg_to_type=json_object_get_int(msgtotype);
			
				json_object_object_get_ex(record,"time",&msgsendtime);
				records[j].msg_send_time=json_object_get_int64(msgsendtime);
				

				json_object_object_get_ex(record,"receiveTime",&msgreceivetime);
				records[j].msg_receive_time=json_object_get_int64(msgreceivetime);


				json_object_object_get_ex(record,"platform",&msgplatform);
				
				if(msgplatform)
				{
					chatResult->records[j].msg_status=json_object_get_int(msgplatform);

				}
				else
				{
					chatResult->records[j].msg_status=1;
				}
				json_object_object_get_ex(record,"sn",&msgsn);
				chatResult->records[j].msg_sn=json_object_get_string(msgsn);
			}
			
			writeDataToPgsql(chatResult, con);
			destory(chatResult);
		}
		else{
			printf("%d\r\n",redisResult->type);
		}
		free(redisResult);
		redisResult=redisCommand(context,"ltrim %s %d %d",redisKey,llen,-1);
		free(redisResult);
	}
	if(resultArray!=NULL)
	{
		json_object_put(resultArray);
	}

	
}
void destory(data_result * result){
	if(result->records!=NULL){
		free(result->records);
		printf("records is release\r\n");
	}
	if(result!=NULL)
		free(result);

}
/*write  data  to pgsql storage the data forever
 purpose: the user want to scan this chat record from server
*/
static void  writeDataToPgsql(data_result * record, PGconn * con){
	
	PGresult   *res;
	int j;

	char * format="insert into msg_record(msg_uuid,msg_type,msg_content,msg_from,msg_from_type,msg_to,msg_to_type,msg_send_time,msg_receive_time,msg_paltform,msg_sn) values('%s','%s','%s','%s',%d,'%s',%d,%d,%d,%d,'%s')";
	msg_record * records=record->records;
	for(j=0;j<record->msg_count;j++)
	{  
		if(strcmp(records[j].msg_type,"offLine")==0)
		{
		//	printf("testabc%d",1);
			continue;
		}
		char sql[10000];
		sprintf(sql,format,records[j].msg_uuid,records[j].msg_type,records[j].msg_content,records[j].msg_from,records[j].msg_from_type,records[j].msg_to,records[j].msg_to_type, records[j].msg_send_time,records[j].msg_receive_time,records[j].msg_platform,records[j].msg_sn);
		printf(sql);
 		res=PQexec(con,sql);
		printf("%d",PQresultStatus(res));
		PQclear(res);
	}	
}

int main(int argc,char *argv[]){
	PGconn *conn = PQconnectdb("user=want_im dbname=im");
	int lib_ver = PQserverVersion(conn);

	printf("Version of libpq: %d\n", lib_ver);
	const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";
    	int port = (argc > 2) ? atoi(argv[2]) : 6379;

    	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
   	context = redisConnectWithTimeout(hostname, port, timeout);
	while(1)
	{
		sleep(1);
		getDataFromRedis(conn);
	}	
	PQfinish(conn);
	redisFree(context);
	return 1;
}
