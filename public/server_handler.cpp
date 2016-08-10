/*
 * server_handler.cpp
 * Author: lixingyi
 */

#include "public/template_packet.h"

#include "public/message.h"
#include "public/server_dispatcher.h"

#include "server_handler.h"
#include "server_manager.h"


using namespace framework ;

ServerHandler::ServerHandler(ServerManager* owner,int16_t local_id):
    m_owner(owner),m_local_server_id(local_id),m_remote_server_id(0),m_last_time(0)
{
    //m_timer.set_owner(this) ;

}

ServerHandler::~ServerHandler()
{
    // TODO Auto-generated destructor stub
}

int ServerHandler::get_packet_info(const char* data,int size,packet_info* pi)
{
    if(size <(int) sizeof(PacketHead))
    {
        pi->size = sizeof(PacketHead) ;
    }
    else
    {
        pi->size = decode_packet_size(data) ;
        pi->type = decode_packet_msg_type(data) ;
        pi->data = data ;
    }

    return 0 ;

}

void ServerHandler::on_closed()
{

    m_owner->free_connection(m_remote_server_id,this) ;

}

void ServerHandler::on_connected()
{
    framework::set_tcp_keepalive(get_id().fd,HEATBEAT_TIME,2,HEATBEAT_TIME/2) ;
    m_remote_server_id = 0 ;
    m_last_time = time(0) ;
    SSRegisterRequest request ;
    request.body.set_node_type(get_node_type(m_local_server_id) ) ;
    request.body.set_node_id(get_node_id(m_local_server_id) ) ;

    this->send(&request,0) ;
}

/*
void ServerHandler::on_timeout(framework::timer_manager* manager)
{
    if(m_remote_server_id ==0) fini() ;
}
*/

int ServerHandler::process_packet(const packet_info* pi)
{
    m_last_time = time(0) ;

    switch(pi->type)
    {
    case SSRegisterRequest::packet_type:
        return process_register_request(pi) ;
    case SSStatusRequest::packet_type:
        return process_status_request(pi) ;
    case SSStatusResponse::packet_type:
        return 0 ;
    default:
        return m_owner->on_server_packet(this,pi) ;
    }

    return 0 ;

}

int ServerHandler::process_register_request(const packet_info* pi)
{
    SSRegisterRequest request ;
    if(request.decode(pi->data,pi->size)!=pi->size ) return -1 ;
    int node_type = request.body.node_type() ;
    int node_id = request.body.node_id() ;
    
    m_remote_server_id = get_server_id( (int8_t)node_type,(int8_t)node_id );
    return m_owner->register_connection(m_remote_server_id,this) ;

}

int ServerHandler::process_status_request(const packet_info* pi)
{
    SSStatusResponse status_response ;
    this->send(&status_response,0) ;
    return 0 ;

}

void ServerHandler::check_connection()
{
    int silence = time(0) - m_last_time ;
    if(silence  < HEATBEAT_TIME )
    {
        return ;
    }
    else if( silence < HEATBEAT_TIME*2 )
    {
        SSStatusRequest request ;
        this->send(&request,0) ;
    }
    else
    {
        fini() ;
    }

}
