#include "common.h"
#include <unistd.h>
#include <pthread.h>
#define MAX_SQl_LEN 5000
#define THREAD_COUNT 2
#define get_string(key,jobj,obj) 
pthread_mutex_t mutexsum;
static char sql[MAX_SQl_LEN];
static void  write_data_to_pg(data_result * record, PGconn * con);
void init_redis_context(redisContext ** context,char * ip_port[]);
void destory(data_result * result);

static  char * ip_port[]={"127.0.0.1","6379"};
static char * redisKey = "IM:allmessagequeue";

/*
	get data from redis the the entity's format is json
	and then write the data to pgsql
 */

static void  get_data_from_redis(){
	clients network_clients;
	init_redis_context(&network_clients.redis_client,ip_port);
	redisContext * context = network_clients.redis_client;
	if(context == NULL){
		return;
	}
	network_clients.pgsql_client= PQconnectdb("user=want_im dbname=im");
	while(1){
		sleep(1);
		json_object * result_array = NULL;
		redisReply * redisResult = NULL;
		if(context != NULL && !context->err){
			//pthread_mutex_lock (&mutexsum);
			redisResult = redisCommand(context,"llen %s",redisKey);
			int llen = redisResult->integer;
			if(llen == 0){
				printf("redis is empty");
				free(redisResult);
				continue;
			}

			printf("%d\r\n",llen);
			free(redisResult);
			redisResult = redisCommand(context,"LRANGE %s %d %d ",redisKey,0,llen-1);		
			redisReply * redis_Result = redisCommand(context,"ltrim %s %d %d",redisKey,llen,-1);
			free(redis_Result);
			//pthread_mutex_unlock (&mutexsum);
			if(redisResult->type == REDIS_REPLY_ARRAY){
				data_result  chatResult = {0};
				msg_record *  records =(msg_record *) malloc(sizeof(msg_record)*redisResult->elements);
				chatResult.msg_count = redisResult->elements;
				result_array = json_object_new_array();
				int j;	
				for (j = 0; j < redisResult->elements; j++) {
					if(!fill_entity(&msg_record[j]),result_array){
						continue;
					}
				}
				chatResult.records = records;
				write_data_to_pg(&chatResult,network_clients.pgsql_client);
				destory(&chatResult);
			}
			else{
				printf("%d\r\n",redisResult->type);
			}
			free(redisResult);
		}
		if(result_array != NULL){
			json_object_put(result_array);
		}

	}
	if(context != NULL){
		redisFree(context);
	}
	PQfinish(network_clients.pgsql_client);
	
}
void  fill_entity(msg_record * m_record,json_object * result_array){

	char * record_content = record->str;				
	json_object * record_content_obj = json_tokener_parse(record_content);
	if(record_content_obj == NULL){
		return 0;
	}
	json_object_array_add(result_array,record);

	json_object * value = NULL;
	json_object_object_get_ex(record_content_obj,"type",&value);
	m_record->msg_type = json_object_get_string(value);
	if(strcmp(records[j].msg_type,"offLine") == 0){
		return 0;
	}

	json_object_object_get_ex(record_content_obj,"id",&value);
	m_record->msg_uuid = json_object_get_string(value);

	json_object_object_get_ex(record_content_obj,"data",&value);
	m_record->msg_content = json_object_get_string(value);		

	json_object_object_get_ex(record_content_obj,"from",&value);

	json_object_object_get_ex(msgfrom,"id",&value);
	m_record->msg_from = json_object_get_string(value);

	json_object_object_get_ex(msgfrom,"type",&value);
	m_record->msg_from_type = json_object_get_int(value);

	json_object_object_get_ex(record_content_obj,"to",&value);
	json_object * to_uid;
	json_object_object_get_ex(record_content_obj,"id",&value);
	m_record->msg_to = json_object_get_string(value);

	
	json_object_object_get_ex(record_content_obj,"type",&value);
	m_record->msg_to_type=json_object_get_int(value);

	json_object_object_get_ex(record_content_obj,"time",&value);
	m_record->msg_send_time=json_object_get_int64(value);
	
	json_object_object_get_ex(record_content_obj,"receiveTime",&value);
	m_record->msg_receive_time = json_object_get_int64(value);

	json_object_object_get_ex(record_content_obj,"platform",&value);
	
	if(value){
		m_record->msg_status = json_object_get_int(value);
	}else{
		m_record->msg_status = 1;
	}
	json_object_object_get_ex(record_content_obj,"sn",&value);
	m_record->msg_sn=json_object_get_string(value);
	return 1;
}
void destory(data_result * result){
	if(result->records != NULL){
		free(result->records);
		printf("records is release\r\n");
	}

}
/*write  data  to pgsql storage the data forever
 purpose: the user want to scan this chat record from server
*/
static void  write_data_to_pg(data_result * record, PGconn * con){
	
	PGresult   *res = NULL;
	int j;
	char * format="insert into msg_record(msg_uuid,msg_type,msg_content,msg_from,msg_from_type,msg_to,\
	msg_to_type,msg_send_time,msg_receive_time,msg_paltform,msg_sn)\
	 values('%s','%s','%s','%s',%d,'%s',%d,%d,%d,%d,'%s')";
	msg_record * records=record->records;
	for(j=0;j<record->msg_count;j++)
	{  
		if(!strcmp(records[j].msg_type,"offLine")){
			continue;
		}
		memset(sql, 0, sizeof(sql));
		sprintf(sql,format,records[j].msg_uuid,records[j].msg_type,records[j].msg_content,\
			records[j].msg_from,records[j].msg_from_type,records[j].msg_to,records[j].msg_to_type,\
			 records[j].msg_send_time,records[j].msg_receive_time,records[j].msg_platform,records[j].msg_sn);
		printf(sql);
 		res = PQexec(con,sql);
		if(res){
			PQclear(res);
		}
	
	}	
}

void init_redis_context(redisContext ** context,char * ip_port[])
{
	const char *hostname = ip_port[1]; 
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
	get_data_from_redis();
	return 1;
}
