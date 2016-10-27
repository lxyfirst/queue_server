/*
 * client_tcp_handler.cpp
 *
 *  Created on: Oct 25, 2015
 *      Author: lxyfirst@163.com
 */

#include "framework/string_util.h"
#include "client_tcp_handler.h"
#include "queue_server.h"
#include "queue_processor.h"
#include "worker.h"

using namespace framework ;


ClientTcpHandler::ClientTcpHandler()
{

    m_idle_timer.set_owner(this) ;

}

ClientTcpHandler::~ClientTcpHandler()
{

}

void ClientTcpHandler::on_timeout(timer_manager* manager)
{
    int idle_time = time(0)- m_last_time ;
    if(idle_time > 60)
    {
        trace_log_format(get_app().get_worker().logger(),"timeout fd:%d",this->get_id().fd) ;
        fini() ;
    }
    else if(idle_time > 20)
    {
        send_heartbeat() ;
    }

    get_app().get_worker().add_timer_after(&m_idle_timer,30) ;
}

void ClientTcpHandler::on_connected()
{
    char addr[32] = {0};
    this->get_remote_addr(addr,sizeof(addr)-1) ;
    debug_log_format(get_app().get_worker().logger(),
            "client connected  host:%s fd:%d",addr,this->get_id().fd) ;

    m_last_time = time(0) ;
    get_app().get_worker().add_timer_after(&m_idle_timer,30) ;

}

void ClientTcpHandler::send_heartbeat()
{
    SSStatusRequest heartbeat ;
    this->send(&heartbeat,0) ;

}

int ClientTcpHandler::on_heartbeat(const framework::packet_info* pi)
{
    SSStatusResponse heartbeat ;
    this->send(&heartbeat,0) ;
    return 0 ;
}

int ClientTcpHandler::get_packet_info(const char* data,int size,framework::packet_info* pi)
{
    if ( data[0] == '{' && size < 65535 )  //web socket
    {
        if( data[size-1] == '}')
        {
            pi->data = data ;
            pi->size =size ;
            pi->type = JSON_PACKET_TYPE ;
        }
        else
        {
            pi->size =size +1;
        }

    }
    else
    {
        //server foward binary data
        if(size < (int)sizeof(PacketHead))
        {
            pi->size = sizeof(PacketHead) ;
        }
        else
        {
            pi->size = decode_packet_size(data) ;
            pi->type = decode_packet_msg_type(data) ;
            pi->data = data ;
        }
    }
              
        
    return 0 ;
    
}

int ClientTcpHandler::process_packet(const packet_info* pi)
{
    m_last_time = time(0) ;

    switch(pi->type)
    {
    case FORWARD_REQUEST:
        return get_app().get_worker().process_forward_request(this,pi) ;
    case FORWARD_RESPONSE:
        return get_app().get_worker().process_forward_response(this,pi) ;
    case JSON_PACKET_TYPE:
        return process_json_request(pi) ;
    case STATUS_REQUEST:
        return on_heartbeat(pi) ;
    case STATUS_RESPONSE :
        return 0 ;
    }

    return -1 ;

}

int ClientTcpHandler::process_json_request(const packet_info* pi)
{
    Json::Value request ;
    if(parse_request(pi->data,pi->data + pi->size,request)!=0) return -1 ;
    debug_log_format(get_app().get_worker().logger(),"recv data size:%d",pi->size) ;

    int action = request[FIELD_ACTION].asInt() ;
    if((!get_app().is_leader() ) && action < ACTION_LOCAL_START)
    {
        SourceData source ;
        source.is_tcp = 1 ;
        source.id = this->get_id();
        return get_app().get_worker().forward_to_leader(source,pi->data,pi->size) ;
    }

    if(QueueProcessor::process(request)==0)
    {
        Json::FastWriter writer ;
        std::string data = writer.write(request) ;
        this->send(data.data(),data.size(),0 ) ;
    }

    return 0 ;
}


void ClientTcpHandler::on_disconnect(int error_type)
{
    get_app().get_worker().del_timer(&m_idle_timer) ;

    debug_log_format(get_app().get_worker().logger(),
            "client closed error_type:%d fd:%d",error_type,this->get_id().fd);
}

void ClientTcpHandler::on_closed()
{
    get_app().get_worker().on_client_closed(this) ;
    
}



