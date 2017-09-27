/*
 * queue_processor.cpp
 *
 *  Created on: 2015��10��27��
 *      Author: dell
 */

#include <string>

#include "queue_processor.h"
#include "worker_util.h"

QueueProcessor::QueueProcessor()
{


}

QueueProcessor::~QueueProcessor()
{

}

int QueueProcessor::fill_response(Document& request,int code,const char* reason)
{
    request.AddMember(FIELD_CODE,code,request.GetAllocator() );

    if(code !=0)
    {
        rapidjson::Value value(reason,strlen(reason)) ;
        request.AddMember(FIELD_REASON,value,request.GetAllocator() ) ;
    }
    return 0 ;
}




int QueueProcessor::process(Document& request)
{
    int action = request[FIELD_ACTION].GetInt() ;

    if(action == ACTION_LIST || action == ACTION_LOCAL_LIST ) return process_list(request) ;

    if(!(request.HasMember(FIELD_QUEUE) && request[FIELD_QUEUE].IsString())) return -1 ;
    std::string queue_name = request[FIELD_QUEUE].GetString() ;

    if(action == ACTION_PRODUCE )
    {
        const QueueNameContainer* queue_list = get_worker().real_queue_name(queue_name);

        if (queue_list)
        {

            for(auto& name : *queue_list)
            {
                Queue* queue = get_worker().get_queue(name) ;
                if(queue)  process_produce(request,*queue) ;
            }

        }
        else
        {
            Queue* queue = get_worker().get_queue(queue_name) ;
            if(queue) process_produce(request,*queue) ;
        }

        request.RemoveMember(FIELD_DATA) ;
        request.RemoveMember(FIELD_DELAY) ;
        request.RemoveMember(FIELD_TTL) ;
        request.RemoveMember(FIELD_RETRY) ;
        return 0 ;
    }

    Queue* queue = get_worker().get_queue(queue_name) ;
    if(queue == NULL) return fill_response(request,-1,"invalid queue") ;

    switch(action)
    {
    case ACTION_CONSUME:
        return process_consume(request,*queue) ;
    case ACTION_CONFIRM:
        return process_confirm(request,*queue) ;
    case ACTION_MONITOR:
    case ACTION_LOCAL_MONITOR:
        return process_monitor(request,*queue) ;
    case ACTION_CONFIG:
        return process_config(request,*queue) ;
    case ACTION_CLEAR:
        return process_clear(request,*queue) ;
    default:
        return fill_response(request,-1,"invalid action") ;
    }

    return 0 ;
}

static const JsonFieldInfo PRODUCE_FILED_LIST{
    {FIELD_DATA,rapidjson::kStringType},
    //{FIELD_DELAY,rapidjson::kNumberType },
} ;

int QueueProcessor::process_produce(Document& request,Queue& queue)
{
    if(!json_check_field(request,PRODUCE_FILED_LIST)) return fill_response(request,-1,"field error") ; ;

    int now = time(0) ;
    int delay = json_get_value(request,FIELD_DELAY,now) ;
    if(delay < now ) delay = now -1 ;

    int ttl = json_get_value(request,FIELD_TTL,delay+3600) ;
    if( delay > ttl ) return fill_response(request,-1,"time error") ; ;

    int retry = json_get_value(request,FIELD_RETRY,0) ;
    if(retry < 0 ) return -1 ;

    int msg_id = queue.produce(request[FIELD_DATA].GetString(),delay,ttl,retry) ;

    if(msg_id >0)
    {
        request.AddMember(FIELD_MSG_ID,msg_id,request.GetAllocator()) ;
        fill_response(request) ;
    }
    else
    {
       fill_response(request,-1,"system error") ;
    }

    return 0 ;
}

int QueueProcessor::process_consume(Document& request,Queue& queue)
{
    std::string data ;
    int msg_id = queue.consume(data) ;
    if(msg_id >0)
    {
        request.AddMember(FIELD_MSG_ID,msg_id,request.GetAllocator()) ;
        request.AddMember(FIELD_DATA,data,request.GetAllocator()) ;
    }

    return fill_response(request) ;
}

int QueueProcessor::process_confirm(Document& request,Queue& queue)
{
    int msg_id = json_get_value(request,FIELD_MSG_ID,0) ;
    if(msg_id >0) queue.erase(msg_id) ;

    return fill_response(request) ;
}

void QueueProcessor::fill_server_info(Document& server_info)
{
    VoteData leader_info , self_info ;
    get_leader_vote_data(leader_info);
    server_info.AddMember("leader_node_id",leader_info.node_id(),server_info.GetAllocator()) ;

    get_self_vote_data(self_info) ;
    server_info.AddMember("node_id", self_info.node_id(),server_info.GetAllocator()) ;
    server_info.AddMember("trans_id",self_info.trans_id(),server_info.GetAllocator()) ;

}

int QueueProcessor::process_monitor(Document& request,Queue& queue)
{
    request.AddMember("size",queue.size(),request.GetAllocator()) ;
    request.AddMember("max_id",queue.max_id(),request.GetAllocator()) ;
    request.AddMember("wait_status",queue.wait_status(),request.GetAllocator()) ;

    fill_server_info(request) ;

    return fill_response(request) ;
}

int QueueProcessor::process_list(Document& request)
{
    rapidjson::Document queue_list ;
    get_worker().list_queue(queue_list ) ;

    request.AddMember("queue_count",queue_list.Size(),request.GetAllocator()) ;
    request.AddMember("queue_list",queue_list,request.GetAllocator()) ;

    fill_server_info(request) ;

    return fill_response(request) ;
}

int QueueProcessor::process_config(Document& request,Queue& queue)
{
    return fill_response(request,-1,"not support") ;
}

int QueueProcessor::process_clear(Document& request,Queue& queue)
{
    //queue.clear() ;
    return fill_response(request,-1,"not support") ;
}

