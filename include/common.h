#include <json.h>
#include <hiredis.h>
#include "stdio.h"
#include <string.h>
#include <libpq-fe.h>
typedef struct{
	const char * msg_uuid;
	const char * msg_type;
	const char * msg_content;
	const char * msg_from;
	int msg_from_type;
	const char * msg_to;
	int msg_to_type;
	long  msg_send_time;
	long msg_receive_time;
	int msg_status;
	int msg_platform;
	const char * msg_sn;

} msg_record;

typedef struct{
	msg_record * records;/*chat_record  data list*/
	int msg_count;

} data_result;


