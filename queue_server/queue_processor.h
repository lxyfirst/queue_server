/*
 * queue_processor.h
 *
 *  Author: lxyfirst@163.com
 */

#pragma once

#include "worker_util.h"
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
    ACTION_GET_LEADER = 8 ,
    ACTION_LOCAL_START = 100,
    ACTION_LOCAL_MONITOR = 104,
    ACTION_LOCAL_LIST = 107,

};

static const char FIELD_CODE[] = "code" ;
static const char FIELD_REASON[] = "reason" ;
static const char FIELD_DATA[] = "data" ;
static const char FIELD_ACTION[] = "action" ;
static const char FIELD_QUEUE[] = "queue" ;
static const char FIELD_DELAY[] = "delay" ;
static const char FIELD_TTL[] = "ttl" ; //  expired time
static const char FIELD_RETRY[] = "retry" ; //  retry seconds
static const char FIELD_MSG_ID[] = "msg_id" ;

class QueueProcessor
{
public:
    QueueProcessor();
    virtual ~QueueProcessor();
    static int fill_response(Document& request,int code=0,const char* reason = "") ;
    static int process(Document& request) ;
    static int process_virtual_produce(Document& request,const std::string& queue_name);
    static int process_produce(Document& request,Queue& queue);
    static int process_consume(Document& request,Queue& queue);
    static int process_confirm(Document& request,Queue& queue);
    static int process_monitor(Document& request,Queue& queue);
    static int process_config(Document& request,Queue& queue);
    static int process_clear(Document& request,Queue& queue);
    static int process_list(Document& request);
    static int process_get_leader(Document& request);
    static void fill_server_info(Document& server_info);

    static bool is_queue_request(int action) ;
};

