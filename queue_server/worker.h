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

struct LocalEventData
{
    int type;
    int timestamp ;
    void* data ;
}  ;

struct SourceData
{
    int is_tcp ;
    framework::sa_in_t addr ;
    framework::tcp_data_handler::connection_id id ;
};

typedef framework::circular_queue<LocalEventData> EventQueue ;

class Worker: public framework::simple_thread
{
public:
    Worker(framework::log_thread& logger);
    virtual ~Worker();

    int on_client_connection(int fd,sa_in_t* addr);
    void on_client_closed(ClientTcpHandler* client_handler) ;
    void free_connection(ClientTcpHandler* client_handler);

    //notify event
    int notify_sync_request(const SyncQueueData& data) ;
    int notify_leader_change() ;

    Queue* get_queue(const string& queue_name);

    //notify callback
    void on_event(int64_t v) ;
    void on_sync_request(void* data) ;
    void on_leader_change(void* data) ;

    //process forward packet
    int process_forward_request(ClientTcpHandler* handler,const framework::packet_info* pi) ;
    int process_forward_response(ClientTcpHandler* handler,const framework::packet_info* pi) ;
    int forward_to_leader(const SourceData& source,const char* data,int size) ;

    framework::log_thread& logger() { return m_logger ; } ;

    int add_timer_after(framework::base_timer* timer,int seconds) ;
    void del_timer(framework::base_timer* timer) ;
    void on_timeout(framework::timer_manager* manager) ;

    void list_queue(Json::Value& queue_list) ;
protected:
    virtual int on_init() ;
    virtual void on_fini() ;
    virtual void run_once() ;

    void process_sync_queue(SyncQueueData& sync_data) ;

    int send_event(int type,void* data) ;
private:
    framework::template_timer<Worker> m_timer ;
    framework::log_thread& m_logger ;
    framework::epoll_reactor m_reactor ;
    framework::timer_manager m_timer_engine ;
    framework::eventfd_handler m_event_handler ;
    EventQueue m_event_queue ;

    QueueManager m_queue_manager ;
    ClientUdpHandler m_udp_handler ;
    ClientTcpHandler m_leader_handler ;
    framework::tcp_acceptor m_client_acceptor ;
    ClientPool m_client_pool ;

};

int parse_request(const char* begin,const char* end,Json::Value& request) ;

