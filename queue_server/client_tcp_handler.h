/*
 * client_tcp_handler.h
 *
 *  Created on: Oct 25, 2014
 *      Author: lxyfirst@163.com
 */

#pragma once

#include <string>

#include "framework/tcp_data_handler.h"
#include "framework/timer_manager.h"

class ClientTcpHandler: public framework::tcp_data_handler
{
public:
    ClientTcpHandler();
    virtual ~ClientTcpHandler();
    enum { JSON_PACKET_TYPE = 1 } ;
    enum { IDLE_TIMEOUT  = 30 };
public:
    
    void on_timeout(framework::timer_manager* manager) ;

    void send_heartbeat() ;
protected:
    int get_packet_info(const char* data,int size,framework::packet_info* pi) ;
    
    int process_packet(const framework::packet_info* pi) ;
    int process_json_request(const framework::packet_info* pi) ;

    void on_disconnect(int error_type) ;

    void on_closed() ;
    
    void on_connected() ;


    int on_heartbeat(const framework::packet_info* pi);

private:
    framework::base_timer m_idle_timer ;
    int m_last_time ;

};

