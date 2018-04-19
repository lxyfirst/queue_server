/*
 * server_manager.h
 *
 *  Created on: Nov 2, 2014
 *      Author: lxyfirst@163.com
 */

#pragma once

#include <tr1/functional>
#include <stdint.h>
#include <map>
#include <set>
#include "server_handler.h"

struct ServerInfo
{
    char host[24] ;
    int port ;
    int16_t node_id ;
    int16_t node_type ;
};


inline int16_t get_server_id(int8_t node_type,uint8_t node_id)
{
    return ((int16_t)node_type << 8) | node_id ;
}

inline int8_t get_node_id(int16_t server_id)
{
    return server_id & 0xFF ;
}

inline int8_t get_node_type(int16_t server_id)
{
    return (server_id >>8) & 0xFF ;
}

typedef std::map<int,ServerInfo> ServerInfoContainer ;

namespace framework
{
    class day_roll_logger ;
    class base_reactor ;
}


class ServerHandler ;

class ServerObserver
{
public:
    virtual int on_server_packet(ServerHandler* handler,const framework::packet_info* pi) {return 0;};

    virtual void on_server_opend(int remote_server_id) {};

    virtual void on_server_closed(int remote_server_id) {};
};

class ServerManager
{
public:
    typedef std::tr1::function<int(ServerHandler*,const framework::packet_info*)> CallbackType ;
    typedef std::map<int,ServerHandler*> ServerContainer ;
    typedef std::set<ServerHandler*> ConnectionContainer ;
public:
    ServerManager();
    virtual ~ServerManager();
    
    int init(framework::day_roll_logger* logger,framework::base_reactor* reactor,ServerObserver* observer,int16_t local_server_id) ;
    
    void fini() ;
    
    /*
     * @brief active connect to remote server
     * @return 0 on success
     */
    int create_connection(const char* host,int port,int remote_server_id) ;
    
    /*
     * @brief active connect to servers if connection not exists
     */
    void check_server_connect(int server_type,const ServerInfoContainer& server_list) ;

    /*
     * @brief accept remote server connection
     * @return 0 on success
     */
    int on_new_connection(int fd) ;

    /*
     * @brief register connection to server id
     * @return 0 on success
     */
    int register_connection(int remote_server_id,ServerHandler* handler) ;

    void unregister_connection(int remote_server_id,ServerHandler* handler) ;

    void check_idle_connection() ;
    
    /*
     * @brief free connection
     */
    void free_connection(ServerHandler* handler) ;

    /*
     * @brief callback by connection when receive packet
     */
    int on_server_packet(ServerHandler* handler,const framework::packet_info* pi)
    {
        //return m_callback(handler,pi) ;
        if(m_observer) return m_observer->on_server_packet(handler,pi) ;
        return 0 ;
    }

    void on_server_opend(int remote_server_id)
    {
        if(m_observer) m_observer->on_server_opend(remote_server_id);
    }

    void on_server_closed(int remote_server_id)
    {
        if(m_observer) m_observer->on_server_closed(remote_server_id);
    }

    /*
     * @brief broadcast packet to servers [low_server_id,high_server_id)
     */
    int broadcast(int low_server_id,int high_server_id,framework::packet* p) ;


    int broadcast(int server_type,framework::packet* p) ;

    /*
     * @brief get server connection by server_id
     * @return server handler
     */
    ServerHandler* get_server(int remote_server_id)
    {
        ServerContainer::iterator it=m_server_container.find(remote_server_id) ;
        if(it!=m_server_container.end()) return it->second ;
        return NULL ;
    }

    int16_t local_server_id() const { return m_local_server_id ; } ;

    int server_count() const { return m_server_container.size() ; } ;
    
private:
    ServerContainer m_server_container ;
    ConnectionContainer m_conn_container ;
    framework::day_roll_logger* m_logger ;
    framework::base_reactor* m_reactor ;
    //CallbackType m_callback ;
    ServerObserver* m_observer ;
    int16_t m_local_server_id ;
};

