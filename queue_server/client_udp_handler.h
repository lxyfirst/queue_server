/*
 * client_udp_handler.h
 * Author: lxyfirst@163.com
 */

#pragma once

#include "framework/udp_data_handler.h"

using framework::sa_in_t ;
using framework::udp_packet ;


class ClientUdpHandler: public framework::udp_data_handler
{
public:
    ClientUdpHandler();
    virtual ~ClientUdpHandler();

    int process_packet(const udp_packet* p);

    int forward_request(const udp_packet* p) ;
};

