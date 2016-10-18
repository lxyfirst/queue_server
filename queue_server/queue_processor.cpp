/*
 * queue_processor.cpp
 *
 *  Created on: 2015Äê10ÔÂ27ÈÕ
 *      Author: dell
 */

#include <string>

#include "queue_processor.h"
#include "queue_server.h"

QueueProcessor::QueueProcessor()
{


}

QueueProcessor::~QueueProcessor()
{

}

int QueueProcessor::fill_response(Value& request,int code,const char* reason)
{
    request[FIELD_CODE] = code ;
    if(code !=0) request[FIELD_REASON] = reason ;
    return 0 ;
}

int QueueProcessor::redirect(Value& request)
{
    const VoteData* leader_info = get_app().leader_vote_data();
    if(leader_info == NULL ||  leader_info->port() < 1) return -1;

    request.removeMember(FIELD_DATA) ;
    request[FIELD_MASTER_HOST]= leader_info->host();
    request[FIELD_MASTER_PORT]= leader_info->port();

    fill_response(request,-2,"redirect") ;
    return 0 ;
}


int QueueProcessor::process(Value& request)
{
    if(!request[FIELD_ACTION].isInt() ) return -1 ;
    int action = request[FIELD_ACTION].asInt() ;

    if((!get_app().is_leader() ) && action < ACTION_LOCAL_START) return redirect(request);

    if(action == ACTION_LIST || action == ACTION_LOCAL_LIST ) return process_list(request) ;

    if(!request[FIELD_QUEUE].isString()) return -1 ;
    std::string queue_name = request[FIELD_QUEUE].asString() ;

    if(action == ACTION_PRODUCE )
    {
        const QueueNameContainer* queue_list = get_app().real_queue_name(queue_name) ;
        if (queue_list)
        {
            for(int i = queue_list->size() -1 ; i >=0 ; --i)
            {
                Queue* queue = get_app().get_worker().get_queue((*queue_list)[i]) ;
                if(queue) process_produce(request,*queue) ;
            }

        }
        else
        {
            Queue* queue = get_app().get_worker().get_queue(queue_name) ;
            if(queue) process_produce(request,*queue) ;
        }

        request.removeMember(FIELD_DATA) ;
        request.removeMember(FIELD_DELAY) ;
        request.removeMember(FIELD_TTL) ;
        request.removeMember(FIELD_RETRY) ;
        return 0 ;
    }

    Queue* queue = get_app().get_worker().get_queue(queue_name) ;
    if(queue == NULL) return fill_response(request,-1,"invalid queue") ;

    switch(action)
    {
    case ACTION_PRODUCE:
        return process_produce(request,*queue) ;
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
int QueueProcessor::process_produce(Value& request,Queue& queue)
{
    if(!request[FIELD_DATA].isString()) return -1 ;

    int now = time(0) ;
    int delay =  request[FIELD_DELAY].isInt() ?  request[FIELD_DELAY].asInt() : now ;
    int ttl = request[FIELD_TTL].isInt() ?  request[FIELD_TTL].asInt() : now + 86400 ;
    if( delay >= ttl ) return -1 ;

    int retry = request[FIELD_RETRY].isInt() ?  request[FIELD_RETRY].asInt() : 0 ;
    if(retry < 0 ) return -1 ;

    int msg_id = queue.produce(request[FIELD_DATA].asString(),delay,ttl,retry) ;

//    request.removeMember(FIELD_DATA) ;
//    request.removeMember(FIELD_DELAY) ;
//    request.removeMember(FIELD_TTL) ;
//    request.removeMember(FIELD_RETRY) ;


    if(msg_id >0)
    {
        request[FIELD_MSG_ID] = msg_id ;
        fill_response(request) ;
    }
    else
    {
       fill_response(request,-1,"system error") ;
    }

    return 0 ;
}

int QueueProcessor::process_consume(Value& request,Queue& queue)
{
    std::string data ;
    int msg_id = queue.consume(data) ;
    if(msg_id >0)
    {
        request[FIELD_MSG_ID] = msg_id ;
        request[FIELD_DATA] = data ;
    }

    return fill_response(request) ;
}

int QueueProcessor::process_confirm(Value& request,Queue& queue)
{
    int msg_id = request[FIELD_MSG_ID].asInt() ;
    queue.erase(msg_id) ;

    return fill_response(request) ;
}

int QueueProcessor::process_monitor(Value& request,Queue& queue)
{
    request["size"] = queue.size() ;
    request["max_id"] = queue.max_id() ;
    request["wait_status"] = queue.wait_status() ;
    request["max_size"] = get_app().queue_size() ;
    Value server_info ;
    get_app().server_info(server_info) ;
    request["server_info"].swap(server_info) ;

    return fill_response(request) ;
}

int QueueProcessor::process_list(Value& request)
{
    Value queue_list ;
    get_app().get_worker().list_queue(queue_list) ;
    request["queue_count"] = queue_list.size() ;
    request["queue_list"].swap(queue_list) ;
    Value server_info ;
    get_app().server_info(server_info) ;
    request["server_info"].swap(server_info) ;

    return fill_response(request) ;
}

int QueueProcessor::process_config(Value& request,Queue& queue)
{
    return fill_response(request,-1,"not support") ;
}

int QueueProcessor::process_clear(Value& request,Queue& queue)
{
    //queue.clear() ;
    return fill_response(request,-1,"not support") ;
}

