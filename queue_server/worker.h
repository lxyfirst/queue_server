/*
 * worker.h
 *
 *  Created on: 2015Äê10ÔÂ30ÈÕ
 *      Author: dell
 */

#pragma once

#include <string>

#include "framework/thread.h"
#include "framework/eventfd_handler.h"
#include "framework/epoll_reactor.h"
#include "framework/circular_queue.h"
#include "framework/log_thread.h"
#include "framework/timer_manager.h"
#include "framework/object_pool.h"
#include "framework/tcp_acceptor.h"
#include "public/system.pb.h"
#include "queue.h"
#include "client_udp_handler.h"
#include "client_tcp_handler.h"

using framework::eventfd_handler ;
using framework::log_thread ;
using framework::poll_reactor ;

using std::string ;

typedef framework::object_pool<ClientTcpHandler> ClientPool ;

typedef struct
{
    int type;
    int timestamp ;
    void* data ;
} LocalEventData ;


typedef framework::circular_queue<LocalEventData> EventQueue ;

class Worker: public framework::simple_thread
{
public:
    Worker(framework::log_thread& logger);
    virtual ~Worker();

    int on_client_connection(int fd,sa_in_t* addr);
    void free_connection(ClientTcpHandler* client_handler);

    int send_sync_request(const SyncQueueData& data) ;

    Queue* get_queue(const string& queue_name);

    void on_event(int64_t v) ;

    framework::log_thread& logger() { return m_logger ; } ;

    int add_timer_after(framework::base_timer* timer,int sencods) ;
    void del_timer(framework::base_timer* timer) ;

    void list_queue(Json::Value& queue_list) ;
protected:
    virtual int on_init() ;
    virtual void on_fini() ;
    virtual void run_once() ;

    void process_sync_queue(SyncQueueData& sync_data) ;

    int send_event(int type,void* data) ;
private:
    framework::log_thread& m_logger ;
    framework::epoll_reactor m_reactor ;
    framework::timer_manager m_timer_engine ;
    framework::eventfd_handler m_event_handler ;
    EventQueue m_event_queue ;

    QueueManager m_queue_manager ;
    ClientUdpHandler m_udp_handler ;
    framework::tcp_acceptor m_client_acceptor ;
    ClientPool m_client_pool ;

};


