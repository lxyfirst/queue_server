/*
 * config_handler.h
 * 
 *  Created on: Oct 25, 2014
 *      Author: lxyfirst@163.com
 */

#pragma once

#include <map>
#include <string>

#include "framework/tcp_data_handler.h"
#include "framework/object_switcher.h"
#include "framework/day_roll_logger.h"

#include "system.pb.h"
#include "server_dispatcher.h"

/*
 * @brief data handler for connection to config server ,watch config data
 * 
 */
class ConfigHandler: public framework::tcp_data_handler
{
public:
    explicit ConfigHandler(framework::day_roll_logger* logger);
    virtual ~ConfigHandler();
    typedef framework::object_switcher<ServerDispatcher> DispatcherSwitcher ;
    typedef std::map<int, DispatcherSwitcher*> DispatcherContainer;
public:
    /*
     * @brief watch server group config data
     */
    int watch_server_group(int node_type) ;

    void remove_server_group(int node_type) ;

    void set_local_server_info(int node_type,int node_id) 
    {
        m_node_type = node_type ;
        m_node_id = node_id ;
    }

    //wrapper ServerDispatcher API for switcher
    /*
     * @brief dispatch to node by key
     * @return node_id
     */
    int dispatch_by_key(int node_type,int key) ;

    const ServerInfo* get_server_info(int node_type,int node_id) ;
    const ServerInfoContainer* get_servers(int node_type) ;


    /*
     * @brief wait data from config server
     * @return 0 if success
     */
    int wait_data(int msec) ;

protected:

    int get_packet_info(const char* data,int size,framework::packet_info* pi) ;
    
    int process_packet(const framework::packet_info* pi) ;
    
    void on_disconnect(int error_type) ;
    
    void on_connected() ;

    void parse_config_data(const ConfigData& config_data) ;

    /*
     * @brief process config data packet
     */
    int process_config_data_response(const framework::packet_info* pi) ;
    
    /*
     * @brief check all config data received
     * @return true if all data received 
     */
    bool is_all_ready()  ;
    
private:
    framework::day_roll_logger* m_logger ;
    DispatcherContainer m_dispatcher_container ;
    int m_node_type ;
    int m_node_id ;

};

