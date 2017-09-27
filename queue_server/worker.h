/*
 * worker.h
 *
 */

#pragma once

#include <string>
#include <vector>
#include <tr1/unordered_map>

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

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"

using rapidjson::Document ;
using rapidjson::Value ;
using rapidjson::StringBuffer;

using framework::eventfd_handler ;
using framework::log_thread ;
using framework::poll_reactor ;

typedef std::vector<std::string> QueueNameContainer ;
typedef std::tr1::unordered_map<std::string,QueueNameContainer > VirtualQueueContainer ;

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

    int init(VirtualQueueContainer& virtual_queue) ;

    //tcp client connection callback
    int on_client_connection(int fd,sa_in_t* addr);
    void on_client_closed(ClientTcpHandler* client_handler) ;
    void free_connection(ClientTcpHandler* client_handler);

    //notify event , called by main thread
    int notify_sync_request(const SyncQueueData& data) ;
    int notify_leader_change() ;
    int notify_queue_config(VirtualQueueContainer& virtual_queue) ;

    //notify callback
    void on_event(int64_t v) ;
    void on_sync_request(void* data) ;
    void on_leader_change(void* data) ;
    void on_queue_config(void* data) ;

    //process forward packet
    int process_forward_request(ClientTcpHandler* handler,const framework::packet_info* pi) ;
    int process_forward_response(ClientTcpHandler* handler,const framework::packet_info* pi) ;
    int forward_to_leader(const SourceData& source,const char* data,int size) ;

    Queue* get_queue(const string& queue_name);
    const QueueNameContainer* real_queue_name(const std::string& name);
    framework::log_thread& logger() { return m_logger ; } ;

    //timer api
    int add_timer_after(framework::base_timer* timer,int seconds) ;
    void del_timer(framework::base_timer* timer) ;
    void on_timeout(framework::timer_manager* manager) ;

    void list_queue(Document& queue_list) ;


protected:
    virtual int on_init() ;
    virtual void on_fini() ;
    virtual void run_once() ;

    /**
     * @brief process sync queue event
     */
    void process_sync_queue(SyncQueueData& sync_data) ;

    /**
     * @brief send event to worker
     */
    int send_event(int type,void* data) ;

    int init_leader_handler() ;
private:
    framework::base_timer m_timer ;
    framework::log_thread& m_logger ;
    framework::epoll_reactor m_reactor ;
    framework::timer_manager m_timer_engine ;
    framework::eventfd_handler m_event_handler ;
    EventQueue m_event_queue ;
    VirtualQueueContainer m_virtual_queue ;
    QueueManager m_queue_manager ;
    ClientUdpHandler m_udp_handler ;
    ClientTcpHandler m_leader_handler ;
    framework::tcp_acceptor m_client_acceptor ;
    ClientPool m_client_pool ;

};

typedef std::map<std::string,int > JsonFieldInfo ;

/*
 * @brief check field and value type of json object
 * @return true on success
 */
bool json_check_field(const Value&json,const JsonFieldInfo& field_list) ;

/**
 * @brief parse request data
 * @return 0 on success
 */
int json_parse_request(const char* begin,const char* end,Document& request) ;

/**
 * @brief serialize json object
 * @return true on success
 */
bool json_encode(const Value& json,StringBuffer& buffer) ;

/**
 * @brief get value from json object
 * @param json json object
 * @param key
 * @param default_value
 * @return int value
 */
int json_get_value(const Value& json,const char* key,int default_value);


