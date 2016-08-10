/*
 * client_tcp_handler.h
 *
 *  Created on: Oct 25, 2014
 *      Author: lxyfirst@163.com
 */

#pragma once

#include <string>

#include "jsoncpp/json.h"
#include "framework/tcp_data_handler.h"
#include "framework/timer_manager.h"

class Worker ;

class ClientTcpHandler: public framework::tcp_data_handler
{
public:
    ClientTcpHandler();
    virtual ~ClientTcpHandler();
    
public:
    
    void on_timeout(framework::timer_manager* manager) ;

protected:
    int get_packet_info(const char* data,int size,framework::packet_info* pi) ;
    
    int process_packet(const framework::packet_info* pi) ;

    void on_disconnect(int error_type) ;

    void on_closed() ;
    
    void on_connected() ;


private:
    framework::template_timer<ClientTcpHandler> m_idle_timer ;

};

