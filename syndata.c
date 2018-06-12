#include "common.h"
#include <unistd.h>
#include <pthread.h>
#define MAXSQlLEN 5000
#define THREAD_COUNT 2;
pthread_mutex_t mutexsum;
static char sql[MAXSQlLEN];
static void  write_data_to_pg(data_result * record, PGconn * con);
void init_redis_context(redisContext ** context,char * ip_port[]);
/*
get data from redis the data format is json
PGconn *conn
read chat data from redis data type is list */

static  char * ip_port[]={"127.0.0.1","6379"};
void destory(data_result * result);

static void  get_data_from_redis(){
	clients network_clients;
	init_redis_context(&network_clients.redis_client,ip_port);
	redisContext * context=network_clients.redis_client;
	network_clients.pgsql_client= PQconnectdb("user=want_im dbname=im");
	while(1){
		sleep(1);
		json_object * resultArray=NULL;
		static char * redisKey = "IM:allmessagequeue";
		redisReply * redisResult=NULL;
		/*if(context!=NULL&&context->err){
			exit(1)
		}*/
		if(context!=NULL&&!context->err){
			//pthread_mutex_lock (&mutexsum);
			redisResult  = redisCommand(context,"llen %s",redisKey);
			int llen=redisResult->integer;
			if(llen==0){
				printf("redis is empty");
				free(redisResult);
				//pthread_mutex_unlock (&mutexsum);
				continue;
			}

			printf("%d\r\n",llen);
			free(redisResult);
			redisResult = redisCommand(context,"LRANGE %s %d %d ",redisKey,0,llen-1);
			#define get_string(key,jobj,obj)  
			redisReply * redis_Result=redisCommand(context,"ltrim %s %d %d",redisKey,llen,-1);
			free(redis_Result);
			//pthread_mutex_unlock (&mutexsum);
			if(redisResult->type==REDIS_REPLY_ARRAY)
			{
				data_result *  chatResult = malloc(sizeof(data_result));
				chatResult->records = malloc(sizeof(msg_record)*redisResult->elements);
				chatResult->msg_count=redisResult->elements;
				resultArray=json_object_new_array();
				const char * recordContent=NULL;
				int j;
				json_object * msguuid,* msgtype,* msgcontent,* msgfrom;
				json_object * msgfromtype, * msgto , *msgtotype,* msgsendtime;
				json_object * msgreceivetime,*msgstatus,*msgplatform,* msgsn;
				msg_record *  records=chatResult->records;	
				for (j = 0; j < redisResult->elements; j++) {
					recordContent=redisResult->element[j]->str;				
					json_object * record=json_tokener_parse(recordContent);
					if(record==NULL)
					{
						continue;
					}
					json_object_object_get_ex(record,"type",&msgtype);
					records[j].msg_type=json_object_get_string(msgtype);
					if(strcmp(records[j].msg_type,"offLine")==0){
						continue;
					}
					json_object_array_add(resultArray,record);
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
				
				writeDataToPgsql(chatResult,network_clients.pgsql_client);
				destory(chatResult);
			}
			else{
				printf("%d\r\n",redisResult->type);
			}
			free(redisResult);
		}
		if(resultArray!=NULL)
		{
			json_object_put(resultArray);
		}

	}	
	redisFree(context);
	PQfinish(network_clients.pgsql_client);
	
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
static void  write_data_to_pg(data_result * record, PGconn * con){
	
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
		//char sql[10000];
		memset(sql, 0, sizeof(sql) );
		sprintf(sql,format,records[j].msg_uuid,records[j].msg_type,records[j].msg_content,records[j].msg_from,records[j].msg_from_type,records[j].msg_to,records[j].msg_to_type, records[j].msg_send_time,records[j].msg_receive_time,records[j].msg_platform,records[j].msg_sn);
		printf(sql);
 		res=PQexec(con,sql);
		printf("%d",PQresultStatus(res));
		PQclear(res);
	}	
}

void init_redis_context(redisContext ** context,char * ip_port[])
{
	const char *hostname =ip_port[1]; 
    	int port = atoi(ip_port[2]);
    	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
   	*context = redisConnectWithTimeout(hostname, port, timeout);

}
void fun_run(){

	/*int t;
	pthread_t thread[2];
	for(t=0; t<THREAD_COUNT; t++) {
		pthread_create(&thread[t],NULL,getDataFromRedis , (void *)t); 
		
	}*/
}

int main(int argc,char *argv[]){
	/*int lib_ver = PQserverVersion(conn);

	printf("Version of libpq: %d\n", lib_ver);
	const char *hostname = (argc > 1) ? argv[1] : "127.0.0.1";
    	int port = (argc > 2) ? atoi(argv[2]) : 6379;

    	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
   	context = redisConnectWithTimeout(hostname, port, timeout);*/
	get_data_from_redis();
	return 1;
}
