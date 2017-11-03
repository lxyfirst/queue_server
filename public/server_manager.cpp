/*
 * server_manager.cpp
 *
 *  Created on: Nov 2, 2014
 *      Author: cjfeng
 */


#include "framework/day_roll_logger.h"

#include "server_handler.h"
#include "server_manager.h"

using namespace framework ;

ServerManager::ServerManager()
{
    // TODO Auto-generated constructor stub

}

ServerManager::~ServerManager()
{
    if(m_conn_container.size() >0) fini() ;
}

int ServerManager::init(day_roll_logger* logger,base_reactor* reactor,ServerObserver* observer,int16_t local_id)
{
    if(logger == NULL || reactor == NULL || local_id == 0) return -1 ;
    
    m_logger = logger ;
    m_reactor = reactor ;
    m_observer = observer ;
    m_local_server_id = local_id ;
    
    return 0 ;
}

void ServerManager::fini()
{
    ConnectionContainer::iterator it = m_conn_container.begin() ;
    while(it!=m_conn_container.end() )
    {
        ServerHandler* server_handler = *it ;
        ++it ;
        server_handler->fini() ;
    }
}

int ServerManager::create_connection(const char* host,int port,int remote_server_id)
{
    ServerContainer::iterator it = m_server_container.find(remote_server_id) ;
    if( it !=m_server_container.end() ) return -1 ;
    
    ServerHandler* server_handler = new ServerHandler(this,m_local_server_id) ;
    if(server_handler == NULL) return -2 ;
    
    if(server_handler->init(*m_reactor,host,port) !=0)
    {
        trace_log_format((*m_logger),"init connection failed , remote_server_id:%#x",remote_server_id) ;
        delete server_handler;
        return -3 ;
    }
    
    trace_log_format((*m_logger),"init connection success , remote_server_id:%#x",remote_server_id) ;
    //m_server_container[remote_server_id] = server_handler ;
    m_conn_container.insert(server_handler) ;
    return 0 ;    
}

int ServerManager::on_new_connection(int fd) 
{
    
    ServerHandler* server_handler = new ServerHandler(this,m_local_server_id) ;
    if(server_handler == NULL) return -1 ;
    if(server_handler->init(*m_reactor,fd) !=0)
    {
        delete server_handler ;
        return -1 ;
    }
    m_conn_container.insert(server_handler) ;
    return 0 ;
}

int ServerManager::register_connection(int remote_server_id,ServerHandler* handler)
{
    ServerContainer::iterator it = m_server_container.find(remote_server_id) ;
    if( (it != m_server_container.end()) && (it->second != handler)  )
    {
        info_log_format((*m_logger),"register server failed , exist remote_server_id:%#x",remote_server_id) ;
        return -1 ;
    }

    info_log_format((*m_logger),"register server success , remote_server_id:%#x",remote_server_id) ;
    m_server_container[remote_server_id] = handler ;
    on_server_opend(remote_server_id) ;

    return 0 ;
}

void ServerManager::unregister_connection(int remote_server_id,ServerHandler* handler)
{
    info_log_format((*m_logger),"unregister server remote_server_id:%#x",remote_server_id) ;
    ServerContainer::iterator it = m_server_container.find(remote_server_id) ;
    if( (it != m_server_container.end() ) && (it->second == handler) )
    {
        m_server_container.erase(it) ;
        on_server_closed(remote_server_id) ;
    }

}

void ServerManager::free_connection(ServerHandler* handler)
{
    m_conn_container.erase(handler) ;
    delete handler ;
}

int ServerManager::broadcast(int low_server_id,int high_server_id,framework::packet* p)
{
    ServerContainer::iterator low_it = m_server_container.lower_bound(low_server_id) ;
    if(low_it == m_server_container.end() ) return 0;

    ServerContainer::iterator high_it = m_server_container.lower_bound(high_server_id) ;

    int send_count = 0 ;
    while( low_it != high_it )
    {
        ServerHandler* handler = low_it->second ;
        ++low_it ;
        if(handler)
        {
            handler->send(p,0) ;
            ++send_count ;
        }
    }

    return send_count ;
}

int ServerManager::broadcast(int server_type,framework::packet* p)
{
    int low_server_id = get_server_id(server_type,1) ;
    int high_server_id = get_server_id(server_type,255) ;
    return broadcast(low_server_id,high_server_id,p) ;
}

void ServerManager::check_server_connect(int server_type,const ServerInfoContainer& server_list)
{
    ServerInfoContainer::const_iterator it = server_list.begin() ;
    for(it = server_list.begin();it!= server_list.end();++it)
    {
        int remote_server_id=get_server_id(server_type,it->first);
        if(it->second.online_status && (m_local_server_id != remote_server_id) )
        {
            create_connection(it->second.host,it->second.port,remote_server_id);
        }
    }

    check_idle_connection() ;

}

void ServerManager::check_idle_connection()
{
    ConnectionContainer::iterator it = m_conn_container.begin() ;
    while(it!=m_conn_container.end() )
    {
        ServerHandler* server_handler = *it ;
        ++it ;
        server_handler->check_connection() ;
    }
}
