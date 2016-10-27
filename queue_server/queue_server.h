/*
 * queue_server.h
 *
 *  Created on: Oct 25, 2014
 *      Author: lxyfirst@163.com
 */

#pragma once

#include <set>
#include <map>
#include <list>
#include <string>
#include <tr1/unordered_map>
#include <vector>
#include "framework/application.h"
#include "framework/tcp_acceptor.h"
#include "framework/day_roll_logger.h"

#include "public/config_handler.h"
#include "public/server_manager.h"
#include "public/template_packet.h"

#include "async_processor_manager.h"
#include "worker.h"

struct NodeData
{
    int8_t node_type ;   // node type
    int8_t node_id ;     // self node id
    int8_t leader_id ;   // leader node id , 0 means no leader
    int8_t vote_status ;
};

struct QueueConfig
{
    int queue_size ;
    int log_size ;
    int sync_rate ;
};

namespace pugi { class xml_node ;} ;
class ClientTcpHandler ;
typedef std::map<int64_t,SyncQueueData> QueueLogContainer ;
typedef std::vector<std::string> QueueNameContainer ;
typedef std::tr1::unordered_map<std::string,QueueNameContainer > VirtualQueueContainer ;

class QueueServer: public framework::application ,public ServerObserver
{
public:
    QueueServer();
    virtual ~QueueServer();
    
public:

    int on_server_connection(int fd,framework::sa_in_t* addr) ;
    int on_sync_queue_request(ServerHandler* handler,const framework::packet_info* pi) ;
    int on_sync_queue_response(ServerHandler* handler,const framework::packet_info* pi) ;

    void on_server_opend(int remote_server_id);
    void on_server_closed(int remote_server_id);
    int on_server_packet(ServerHandler* handler,const framework::packet_info* pi);
    int on_fsm_response(ServerHandler* handler,const framework::packet_info* pi);
    int on_other_vote(ServerHandler* handler,const framework::packet_info* pi);
    int on_vote_success(ServerHandler* handler,const framework::packet_info* pi);

    framework::day_roll_logger& logger() { return m_logger ; } ;
    
    const VoteData& self_vote_data() { return m_self_vote_info ; } ;
    const VoteData* leader_vote_data()
    {
        const VoteData& vote_data = m_leader_vote_info.active() ;
        if(m_node_info.leader_id <1) return NULL ;
        return vote_data.node_id() == m_node_info.leader_id ? &vote_data : NULL ;
    }

    int majority_count() const { return (m_cluster_info.size()+1) >>1  ; } ;

    bool is_leader() const { return m_node_info.node_id == m_node_info.leader_id ; } ;

    void set_leader(const VoteData& vote_data)  ;

    ServerHandler* get_leader() ;

    int broadcast(framework::packet* p) { return m_server_manager.broadcast(m_node_info.node_type,p); } ;

    AsyncProcessorManager& async_manager() { return m_processor_manager ; } ;

    void stop_vote() ;

    void on_queue_log(SyncQueueData& log_data) ;
    void on_event(int64_t v) ;


    void server_info(Json::Value& info) ;

    int queue_size() const { return m_queue_config.queue_size ; } ;
    int log_size() const { return m_queue_config.log_size ; } ;

    const QueueNameContainer* real_queue_name(const std::string& virtual_name)
    {
        VirtualQueueContainer& container = m_virtual_queue.active() ;
        if( container.count(virtual_name) <1 ) return NULL ;
        return &container[virtual_name] ;
    }

    Worker& get_worker() { return m_worker ; } ;
    int send_event(SyncQueueData* data) ;
protected:
    int load_cluster_config(const pugi::xml_node& root) ;
    int load_reload_config(const pugi::xml_node& root) ;

    int on_init() ;

    int on_reload() ;
    
    void on_fini() ;

    void on_delay_stop() ;

    void on_timer() ;

    int wait_config() ;

    int start_vote() ;

    void check_leader();

    void on_sync_timeout(framework::timer_manager* manager) ;

    void try_sync_queue() ;

    const SyncQueueData& update_queue_log(SyncQueueData& sync_data);
private:
    framework::template_timer<QueueServer,&QueueServer::on_sync_timeout> m_sync_timer ;
    framework::day_roll_logger m_logger ;
    framework::tcp_acceptor m_server_acceptor ;
    framework::eventfd_handler m_event_handler ;
    EventQueue m_event_queue ;
    framework::log_thread m_log_thread ;
    Worker m_worker ;

    ServerManager m_server_manager ;

    ServerInfoContainer m_cluster_info ;   // node list in cluster
    ServerInfo m_self_info ;               // self info in cluster
    framework::object_switcher<VirtualQueueContainer> m_virtual_queue ;
    QueueLogContainer m_queue_log ;
    AsyncProcessorManager m_processor_manager ;
    QueueConfig m_queue_config ;
    VoteData m_self_vote_info ;     // self vote info
    framework::object_switcher<VoteData> m_leader_vote_info ;   // leader vote info
    NodeData m_node_info ;          //  node info and status
    int m_sync_counter ;
    int m_sync_time ;

};

int parse_request(const char* begin,const char* end,Json::Value& request) ;

DECLARE_APPLICATION_INSTANCE(QueueServer) ;

