/*
 * queue_processor.h
 *
 *  Author: lxyfirst@163.com
 */

#pragma once

#include "jsoncpp/json.h"

using Json::Value ;
class Queue ;

enum
{
    ACTION_PRODUCE = 1 ,
    ACTION_CONSUME = 2 ,
    ACTION_CONFIRM = 3 ,
    ACTION_MONITOR = 4 ,
    ACTION_CONFIG = 5 ,
    ACTION_CLEAR = 6 ,
    ACTION_LIST = 7 ,
    ACTION_LOCAL_START = 100,
    ACTION_LOCAL_MONITOR = 104,
    ACTION_LOCAL_LIST = 107,

};

static const char* FIELD_CODE = "code" ;
static const char* FIELD_REASON = "reason" ;
static const char* FIELD_DATA = "data" ;
static const char* FIELD_MASTER_PORT = "master_port" ;
static const char* FIELD_MASTER_HOST = "master_host" ;
static const char* FIELD_ACTION = "action" ;
static const char* FIELD_QUEUE = "queue" ;
static const char* FIELD_DELAY = "delay" ;
static const char* FIELD_TTL = "ttl" ;
static const char* FIELD_RETRY = "retry" ;
static const char* FIELD_MSG_ID = "msg_id" ;

class QueueProcessor
{
public:
    QueueProcessor();
    virtual ~QueueProcessor();
    static int fill_response(Value& request,int code=0,const char* reason = "") ;
    static int process(Value& request) ;
    static int process_produce(Value& request,Queue& queue);
    static int process_consume(Value& request,Queue& queue);
    static int process_confirm(Value& request,Queue& queue);
    static int process_monitor(Value& request,Queue& queue);
    static int process_config(Value& request,Queue& queue);
    static int process_clear(Value& request,Queue& queue);
    static int process_list(Value& request);
    static void fill_server_info(Value& server_info);
};

