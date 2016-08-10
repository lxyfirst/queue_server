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
    trace_log_format(get_app().get_worker().logger(),"timeout fd:%d",this->get_id().fd) ;
    fini() ;
}

void ClientTcpHandler::on_connected()
{
    char addr[32] = {0};
    this->get_remote_addr(addr,sizeof(addr)-1) ;
    debug_log_format(get_app().get_worker().logger(),"connected from %s",addr) ;

    get_app().get_worker().add_timer_after(&m_idle_timer,3000) ;

}

int ClientTcpHandler::get_packet_info(const char* data,int size,framework::packet_info* pi)
{
    if ( data[0] == '{' && size < 65535 )  //web socket
    {
        if( data[size-1] == '}')
        {
            pi->data = data ;
            pi->size =size ;
            pi->type = 1 ;
        }
        else
        {
            pi->size =size +1;
        }

    }
    else
    {
        return -1 ;
    }
              
        
    return 0 ;
    
}

int ClientTcpHandler::process_packet(const packet_info* pi)
{

    Json::Value request ;
    if(parse_request(pi->data,pi->data + pi->size,request)!=0) return -1 ;
    debug_log_format(get_app().get_worker().logger(),"recv data size:%d",pi->size) ;


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
    int error_no = get_errno() ;
    debug_log_format(get_app().get_worker().logger(),"client closed error_type:%d error_no:%d",error_type,error_no);
}

void ClientTcpHandler::on_closed()
{
    get_app().get_worker().del_timer(&m_idle_timer) ;

    get_app().get_worker().free_connection(this);
    
}



