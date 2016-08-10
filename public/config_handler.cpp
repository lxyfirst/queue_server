/*
 * config_handler.cpp
 *
 *  Created on: Oct 25, 2014
 *      Author: lxyfirst@gmail.com
 */

#include "framework/base_reactor.h"
#include "framework/time_util.h"
#include "pugixml/pugixml.hpp"
#include "public/message.h"
#include "config_handler.h"

using namespace framework ;


ConfigHandler::ConfigHandler(day_roll_logger* logger):m_logger(logger)
{
    // TODO Auto-generated constructor stub

}

ConfigHandler::~ConfigHandler()
{
    DispatcherContainer::iterator it =m_dispatcher_container.begin();
    for(;it!=m_dispatcher_container.end();++it )
    {
        delete it->second ;
    }
}

int ConfigHandler::get_packet_info(const char* data,int size,framework::packet_info* pi)
{

        //binary
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

int ConfigHandler::process_packet(const packet_info* pi) 
{
    switch(pi->type)
    {
    case SSConfigDataResponse::packet_type:
        return process_config_data_response(pi) ;
    }
    return 0 ;
}

int ConfigHandler::process_config_data_response(const packet_info* pi)
{
    SSConfigDataResponse response;
    if(response.decode(pi->data,pi->size) < 0 ) return -1 ;
    

    for(int i=0; i< response.body.data_list_size() ; ++i)
    {
        const ConfigData& config_data = response.body.data_list(i) ;
        
        parse_config_data(config_data) ;
    }
    
    
    return 0 ;
}

void ConfigHandler::parse_config_data(const ConfigData& config_data)
{
    int node_type = config_data.node_type() ;
    const std::string& data = config_data.data() ;
    if(m_logger) info_log_format((*m_logger),"parse config node_type:%d size:%d",node_type,data.size()) ;

    DispatcherContainer::iterator it = m_dispatcher_container.find(node_type);
    if(it == m_dispatcher_container.end() )
    {
        return ;
    }

    if(it->second->active().is_ready() && config_data.protect_mode()!=0)
    {
        //config_server in protoect mode ,ignore
        return ;
    }

    ServerIdContainer server_id_list ;
    for(int i = 0 ; i < config_data.online_node_list_size();++i)
    {
        server_id_list.insert(config_data.online_node_list(i)) ;
    }
    


    
    if(it->second->backup().load_config(data,server_id_list)!=0)
    {
        if(m_logger) info_log_format((*m_logger),"load config failed  node_type:%d",node_type) ;
        return ;
    }
    
    it->second->switch_object() ;
    
}


void ConfigHandler::on_connected()
{
    framework::set_tcp_keepalive(get_id().fd,30,1) ;
    if(m_logger) debug_log_string((*m_logger),"connected to config server") ;
    
    if(m_dispatcher_container.size()<1) return ;


    //register
    SSRegisterRequest register_request ;
    register_request.body.set_node_type(m_node_type ) ;
    register_request.body.set_node_id(m_node_id) ;
    this->send(&register_request,0) ;

    //subscribe
    SSConfigDataRequest request ;
    DispatcherContainer::iterator it =m_dispatcher_container.begin();
    for(;it!=m_dispatcher_container.end();++it )
    {
        request.body.add_node_type_list(it->first) ;

    }

    this->send(&request,0) ;

}

void ConfigHandler::on_disconnect(int error_type) 
{
    if(m_logger) debug_log_format((*m_logger),"closed to config server error_type:%d",error_type) ;
}


int ConfigHandler::watch_server_group(int node_type)
{
    if(m_dispatcher_container.count(node_type) >0) return -1 ;
    DispatcherSwitcher* switcher = new DispatcherSwitcher()  ;
    if(switcher == NULL) return -1 ;
    m_dispatcher_container[node_type] = switcher ;

    if(is_connected() )
    {
        SSConfigDataRequest request ;
        request.body.add_node_type_list(node_type) ;
        this->send(&request,0) ;

    }

    return 0 ;
}

void ConfigHandler::remove_server_group(int node_type)
{
    DispatcherContainer::iterator it =m_dispatcher_container.find(node_type) ;
    if(it!= m_dispatcher_container.end())
    {
        delete it->second ;
    }

    m_dispatcher_container.erase(it) ;
}

bool ConfigHandler::is_all_ready()
{
    DispatcherContainer::iterator it =m_dispatcher_container.begin();
    for(;it!=m_dispatcher_container.end();++it )
    {
        if(!it->second->active().is_ready() ) return false ;
    }

    return true ;
}

int ConfigHandler::dispatch_by_key(int node_type,int key)
{
    DispatcherContainer::iterator it =m_dispatcher_container.find(node_type);
    if (it == m_dispatcher_container.end()) return 0 ;
    return it->second->active().dispatch_by_key(key) ;

}

const ServerInfo* ConfigHandler::get_server_info(int node_type,int node_id)
{
    DispatcherContainer::iterator it =m_dispatcher_container.find(node_type);
    if (it == m_dispatcher_container.end()) return NULL ;
    return it->second->active().get_server(node_id) ;
}

const ServerInfoContainer* ConfigHandler::get_servers(int node_type)
{
    DispatcherContainer::iterator it =m_dispatcher_container.find(node_type);
    if (it == m_dispatcher_container.end()) return NULL ;
    return it->second->active().get_servers() ;
}


int ConfigHandler::wait_data(int msec)
{
    do
    {
        if(is_closed()) return -1 ;

        int64_t begin_ms = time_ms() ;
        get_reactor()->run_once(msec) ;
        msec -= time_ms() - begin_ms ;

    }while (msec > 0 && (!is_all_ready() ) ) ;

    if(is_all_ready() ) return 0 ;

    return -1 ;
}
