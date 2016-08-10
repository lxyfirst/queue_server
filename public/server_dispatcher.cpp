/*
 * server_dispatcher.cpp
 * Author: lixingyi
 */

#include <string.h>
#include "pugixml/pugixml.hpp"
#include "server_dispatcher.h"

ServerDispatcher::ServerDispatcher():m_algorithm(NULL)
{
    // TODO Auto-generated constructor stub

}

ServerDispatcher::~ServerDispatcher()
{
    reset() ;
}

bool ServerDispatcher::is_ready() const
{
    return (m_server_list.size() >0) && (m_algorithm!=NULL);
}

void ServerDispatcher::reset()
{

    if(m_algorithm!=NULL)
    {
        delete m_algorithm ;
        m_algorithm = NULL ;
    }

    m_server_list.clear() ;
}

int ServerDispatcher::dispatch_by_key(int key) const
{
    if(m_algorithm ==NULL) return 0 ;
    return m_algorithm->dispatch(key) ;

}

int ServerDispatcher::load_config(const std::string& data,const ServerIdContainer& server_id_list)
{
    pugi::xml_document xml_config ;

    if(!xml_config.load(data.c_str(),0) ) return -1 ;

    pugi::xml_node node = xml_config.child("server_group") ;
    
    ServerInfoContainer server_list ;
    //attributes: id ,host ,port
    for (pugi::xml_node item = node.first_child(); item;item = item.next_sibling())
    {
        int node_id = item.attribute("id").as_int() ;

        ServerInfo& server_info =server_list[node_id] ;
        server_info.host[sizeof(server_info.host)-1] = '\0' ;
        strncpy(server_info.host,item.attribute("host").value(),sizeof(server_info.host)-1 ) ;
        server_info.node_id = node_id ;
        server_info.port = item.attribute("port").as_int() ;
        server_info.online_status = server_id_list.count(node_id) ;
    }

    DispatchAlgorithm* algorithm = create_algorithm(node.attribute("algorithm").value()) ;
    if(algorithm==NULL) return -1 ;
    if( algorithm->on_init(server_list)!=0)
    {
        delete algorithm ;
        return -2;
    }
    
    m_server_list.swap(server_list) ;
    if(m_algorithm !=NULL) delete m_algorithm ;
    m_algorithm = algorithm ;
    
    return 0 ;
}


DispatchAlgorithm* ServerDispatcher::create_algorithm(const char* name)
{

    if(strcmp(name,"hash")==0)
    {
        return new HashAlgorithm() ;
    }
    else if( strcmp(name,"consistent_hash")==0)
    {
        return new ConsistentHashAlgorithm() ;
    }

    return NULL ;

}



int HashAlgorithm::on_init(const ServerInfoContainer& data)
{
    if(data.size() < 1) return -1 ;

    m_node_list.clear();
    for(ServerInfoContainer::const_iterator it=data.begin();it!=data.end();++it)
    {
        if(it->second.online_status == 0) continue ;
        m_node_list.push_back(it->first) ;
    }

    return 0 ;
}

int HashAlgorithm::dispatch(int key)
{
    if(m_node_list.size() ==0) return 0 ;

    return m_node_list[key % m_node_list.size()] ;
}


int ConsistentHashAlgorithm::on_init(const ServerInfoContainer& data)
{
    //todo consistent hash data structure
    return 0 ;
}

int ConsistentHashAlgorithm::dispatch(int key)
{
    //todo consistent hash algorithm
    return 0 ;
}
