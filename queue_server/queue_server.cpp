/*
 * queue_server.cpp
 *
 *  Created on: Oct 25, 2014
 *      Author: lxyfirst@gmail.com
 */

#include "pugixml/pugixml.hpp"
#include "framework/system_util.h"
#include "framework/time_util.h"
#include "framework/member_function_bind.h"
#include "public/message.h"

#include "queue_server.h"
#include "queue_processor.h"

using namespace framework ;


QueueServer::QueueServer():m_worker(m_log_thread)
{
   m_sync_timer.set_owner(this) ;

}

QueueServer::~QueueServer()
{

}


int QueueServer::on_init()
{

    pugi::xml_document xml_config ;
    if(!xml_config.load_file(config_file()))
    {
        error_return(-1,"load config failed") ;
    }
    pugi::xml_node root = xml_config.child("root") ;
    if(!root) error_return(-1,"missing <root> node") ;

    if(load_reload_config(root)!=0) return -1 ;

    if(m_log_thread.start() !=0) error_return(-1,"init log thread failed") ;

    if(load_cluster_config(root)!=0) return -1 ;
    
    //init event queue
    if(m_event_queue.init(log_size())!=0) error_return(-1,"init queue failed") ;
    eventfd_handler::callback_type callback = member_function_bind(&QueueServer::on_event,this) ;
    if(m_event_handler.init(reactor(),callback )!=0 )
    {
        error_return(-1,"init eventfd failed") ;
    }

    //init worker thread
    if(m_worker.start()!=0) error_return(-1,"init worker failed") ;

    set_delay_stop_timer(1000) ;
    set_app_timer(10000) ;
    info_log_string(m_logger,"system started") ;
    return 0 ;
}

int QueueServer::load_cluster_config(const pugi::xml_node& root)
{
    //load cluster config
    pugi::xml_node node = root.child("cluster") ;
    for (pugi::xml_node item = node.first_child(); item;item = item.next_sibling())
    {
        int node_id = item.attribute("id").as_int() ;

        ServerInfo& server_info =m_cluster_info[node_id] ;
        server_info.host[sizeof(server_info.host)-1] = '\0' ;
        strncpy(server_info.host,item.attribute("host").value(),sizeof(server_info.host)-1 ) ;
        server_info.node_id = node_id ;
        server_info.port = item.attribute("port").as_int() ;
        server_info.online_status = 1;
    }

    //load self node config
    node = root.child("node_info") ;
    m_node_info.node_type = node.attribute("node_type").as_int(0);
    m_node_info.node_id = node.attribute("node_id").as_int();
    if(m_node_info.node_type <0 ) error_return(-1,"invalid gorup_type:%d",m_node_info.node_type);
    if(m_node_info.node_id <1 ) error_return(-1,"invalid node id:%d",m_node_info.node_id);

    std::string client_host = node.attribute("host").as_string();
    int client_port = node.attribute("port").as_int();

    m_self_vote_info.set_node_id(m_node_info.node_id) ;
    m_self_vote_info.set_vote_id(0) ;
    m_self_vote_info.set_host( client_host ) ;
    m_self_vote_info.set_port(client_port ) ;

    int16_t local_server_id = get_server_id(m_node_info.node_type,m_node_info.node_id) ;
    m_server_manager.init(&m_logger,&reactor(),this,local_server_id) ;
    ServerInfoContainer::iterator it = m_cluster_info.find(m_node_info.node_id) ;
    if(it == m_cluster_info.end() ) error_return(-1,"cannot get node info") ;
    m_self_info = it->second ;
    m_cluster_info.erase(it) ;


    tcp_acceptor::callback_type server_callback = member_function_bind(&QueueServer::on_server_connection,this) ;
    if(m_server_acceptor.init(reactor(),m_self_info.host,m_self_info.port,server_callback )!=0)
    {
        error_return(-1,"init server acceptor failed") ;
    }

    return 0 ;

}

int QueueServer::load_reload_config(const pugi::xml_node& root)
{
    pugi::xml_node node = root.child("log") ;
    if(m_logger.init(node.attribute("prefix").value(),node.attribute("level").as_int() ) !=0 )
    {
        error_return(-1,"init logger failed") ;
    }

    m_log_thread.init(node.attribute("prefix").value(),node.attribute("level").as_int());

    node = root.child("queue_config") ;
    m_queue_config.queue_size = node.attribute("queue_size").as_int(10000);
    m_queue_config.log_size = node.attribute("log_size").as_int(100000);
    m_queue_config.sync_rate = node.attribute("sync_rate").as_int(1000);
    if(m_queue_config.queue_size < 128 || m_queue_config.log_size < 128 ) error_return(-1,"invalid size");
    if( m_queue_config.sync_rate < 1) error_return(-1,"invalid limit") ;

    VirtualQueueContainer virtual_queue  ;

    for(pugi::xml_node queue = root.child("virtual_queue");queue ; queue= queue.next_sibling("virtual_queue") )
    {
        const char* virtual_name = queue.attribute("name").value();
        if(strlen(virtual_name) < 1 ) continue ;
        QueueNameContainer& name_list = virtual_queue[virtual_name];
        for (pugi::xml_node item = queue.first_child(); item;item = item.next_sibling())
        {
            const char* real_name =item.attribute("name").value() ;
            if(strlen(real_name) >0 ) name_list.push_back(real_name) ;
        }
    }

    m_worker.init(virtual_queue) ;


    return 0 ;
}

int QueueServer::on_reload()
{
    pugi::xml_document xml_config ;
    if(!xml_config.load_file(config_file()))
    {
        error_return(-1,"load config failed") ;
    }
    pugi::xml_node root = xml_config.child("root") ;
    if(!root) error_return(-1,"missing <root> node") ;

    m_logger.fini() ;
    load_reload_config(root) ;

    info_log_string(m_logger,"system reload success") ;
    return 0 ;
}

void QueueServer::on_fini()
{
    m_server_manager.fini() ;
    m_worker.join() ;
    google::protobuf::ShutdownProtobufLibrary();
    info_log_string(m_logger,"system stopped") ;

}

void QueueServer::on_delay_stop()
{
    m_server_acceptor.fini() ;
    m_worker.stop() ;
}

void QueueServer::on_timer()
{
    check_leader() ;

    m_server_manager.check_server_connect(m_node_info.node_type,m_cluster_info) ;

}


void QueueServer::check_leader()
{
    if(!is_running() ) return ;

    //no cluster
    if(m_cluster_info.size() ==0)
    {
        m_node_info.leader_id = m_node_info.node_id ;
        return ;
    }

    // connected to majority nodes
    if( m_server_manager.server_count() >= majority_count() )
    {
        if( !is_leader() && get_leader() == NULL )
        {
            m_node_info.leader_id  = 0 ;
            start_vote() ;
        }
    }
    else
    {
        m_node_info.leader_id  = 0 ;
    }

}

void QueueServer::set_leader(const VoteData& vote_data)
{
    m_node_info.leader_id = vote_data.node_id() ;

    m_leader_vote_info.backup().CopyFrom(vote_data) ;
    m_leader_vote_info.switch_object() ;
    m_worker.notify_leader_change() ;
    try_sync_queue() ;

    info_log_format(m_logger,"new leader node_id:%d",m_node_info.leader_id) ;
}

ServerHandler* QueueServer::get_leader()
{
    if(m_node_info.leader_id <1 ) return NULL ;

    return m_server_manager.get_server(get_server_id(m_node_info.node_type,m_node_info.leader_id)) ;
}


int QueueServer::start_vote()
{
    if(m_node_info.vote_status != 0  ) return 0 ;

    m_node_info.vote_status = 1 ;

    base_fsm* fsm = m_processor_manager.create_fsm(StartVoteProcessor::TYPE);
    if(fsm)
    {
        m_self_vote_info.set_vote_id(m_self_vote_info.vote_id() +1) ;
        fsm->enter(&m_processor_manager,STATUS_INIT,&m_self_vote_info) ;
        return 0 ;
    }

    return -1 ;

}

void QueueServer::stop_vote()
{
    m_node_info.vote_status = 0 ;
}



int QueueServer::on_server_connection(int fd,sa_in_t* addr)
{
    return m_server_manager.on_new_connection(fd) ;

}


void QueueServer::on_server_opend(int remote_server_id)
{
    check_leader() ;
    if(m_node_info.node_type == get_node_type(remote_server_id) )
    {
        m_push_sync_set.insert(remote_server_id) ;
    }
}

void QueueServer::on_server_closed(int remote_server_id)
{
    m_push_sync_set.erase(remote_server_id) ;
    check_leader() ;
}


int QueueServer::on_server_packet(ServerHandler* handler,const framework::packet_info* pi)
{
    debug_log_format(m_logger,"on_server_packet type:%d",pi->type) ;
    switch(pi->type)
    {
    case VOTE_REQUEST:
        return on_other_vote(handler,pi) ;
    case VOTE_RESPONSE:
        return on_fsm_response(handler,pi) ;
    case VOTE_NOTIFY:
        return on_vote_success(handler,pi) ;
    case SYNC_QUEUE_REQUEST:
        return on_sync_queue_request(handler,pi) ;
    case SYNC_QUEUE_RESPONSE:
        return on_sync_queue_response(handler,pi) ;

    default:
        ;
    }


    return 0 ;
}

int QueueServer::on_fsm_response(ServerHandler* handler,const framework::packet_info* pi)
{
    SSHead sshead ;
    sshead.decode(pi->data + sizeof(PacketHead),sizeof(sshead)) ;
    int seq = sshead.seq ;

    base_fsm* processor = m_processor_manager.get_fsm(seq) ;
    if(processor)
    {
        return processor->enter(&m_processor_manager,STATUS_WAIT_DATA,(void*)pi) ;
    }

    return 0 ;
}

static bool vote_data_gt(const VoteData& a , const VoteData& b)
{
    if(a.trans_id() > b.trans_id() ) return true ;
    else if (a.trans_id() < b.trans_id()) return false ;

    if(a.vote_id() > b.vote_id() ) return true ;
    else if(a.vote_id() < b.vote_id()) return false ;

    if(a.node_id() > b.node_id() ) return true ;

    return false ;

}

int QueueServer::on_other_vote(ServerHandler* handler,const framework::packet_info* pi)
{
    SSVoteRequest request ;
    if(request.decode(pi->data,pi->size)!=pi->size) return -1 ;
    info_log_format(m_logger,"recv vote vote_id:%d node_id:%d",request.body.vote_id(),request.body.node_id()) ;

    SSVoteResponse response ;
    response.head = request.head ;

    if(is_leader() || get_leader() )
    {
        response.body.set_error_code(EC_VOTE_LEADER_EXIST) ;
        response.body.mutable_data()->CopyFrom(m_leader_vote_info.active()) ;
    }
    else if( vote_data_gt(request.body,m_self_vote_info)  &&
        vote_data_gt(request.body,m_leader_vote_info.active() )  )
    {
        response.body.set_error_code(EC_SUCCESS) ;
        m_leader_vote_info.backup().CopyFrom(request.body) ;
        m_leader_vote_info.switch_object();

    }
    else
    {
        response.body.set_error_code(EC_VOTE_FAILED) ;
        response.body.mutable_data()->CopyFrom(m_leader_vote_info.active() ) ;
    }


    return handler->send(&response,0) ;

}

int QueueServer::on_vote_success(ServerHandler* handler,const framework::packet_info* pi)
{
    SSVoteNotify notify ;
    if(notify.decode(pi->data,pi->size)!= pi->size) return -1 ;

    set_leader(notify.body) ;

    return 0 ;
}



//from slave to leader
int QueueServer::on_sync_queue_request(ServerHandler* handler,const framework::packet_info* pi)
{
    if(!is_leader() ) return 0 ;

    SSSyncQueueRequest request ;
    if(request.decode(pi->data,pi->size)!= pi->size) return -1 ;

    int64_t last_trans_id = request.body.last_trans_id() ;
    debug_log_format(m_logger,"sync request last_trans_id:%lld",last_trans_id) ;
    if(last_trans_id >= m_self_vote_info.trans_id() )
    {
        //no data to sync , change to push mode
        m_push_sync_set.insert(handler->remote_server_id()) ;
        info_log_format(m_logger,"sync data finished server_id:%#x",handler->remote_server_id()) ;
        return 0 ;
    }

    QueueLogContainer::iterator it = m_queue_log.upper_bound(last_trans_id) ;
    if(it!=m_queue_log.end())
    {
        SSSyncQueueResponse response ;
        response.head = request.head ;
        response.body.CopyFrom(it->second) ;
        response.body.set_last_trans_id(last_trans_id) ;
        if(handler->send(&response,0)!=0) return -1 ;
        if (it->second.trans_id() - last_trans_id > 1)
        {
            m_push_sync_set.erase(handler->remote_server_id()) ;
            info_log_format(m_logger,"sync skip server_id:%#x trans_id %lld to %lld",
                    handler->remote_server_id(),last_trans_id,it->second.trans_id());
        }
    }
    else
    {
        m_push_sync_set.insert(handler->remote_server_id()) ;
        error_log_format(m_logger,"maybe it's a bug") ;
    }

    return 0 ;
}

//from leader to slave , notify or response
int QueueServer::on_sync_queue_response(ServerHandler* handler,const framework::packet_info* pi)
{

    int leader_server_id =get_server_id(m_node_info.node_type,m_node_info.leader_id) ;
    if(handler->remote_server_id() != leader_server_id) return 0;

    SSSyncQueueResponse response ;
    if(response.decode(pi->data,pi->size)!= pi->size) return -1 ;

    int64_t trans_id = response.body.trans_id() ;
    debug_log_format(m_logger,"sync notify trans_id:%lld",trans_id) ;
    if(m_self_vote_info.trans_id() == response.body.last_trans_id() )
    {
        //sync notify or response is continuous , update local queue and log
        if(m_worker.notify_sync_request(response.body) !=0 )
        {
            warn_log_format(m_logger,"sync to queue failed");
            return -1 ;
        }

        //sync to local queue log
        update_queue_log(response.body) ;

        m_self_vote_info.set_trans_id(trans_id) ;

    }
    else if(m_self_vote_info.trans_id() < response.body.last_trans_id())
    {
        //sync notify not continus , ignore and try again
        response.head.seq = SYNC_TYPE_PULL ;
    }

    //try sync again
    //seq=1 means continuous sync or pull sync
    //seq=0 means oneshot sync or push sync
    if(response.head.seq == SYNC_TYPE_PULL) try_sync_queue() ;


    return 0 ;
}

void QueueServer::on_sync_timeout(framework::timer_manager* manager)
{
    try_sync_queue() ;
}

void QueueServer::try_sync_queue()
{
    int now = time(0) ;
    if(m_sync_time != now)
    {
        m_sync_time = now ;
        m_sync_counter = 0 ;
    }
    else
    {
        ++m_sync_counter ;
    }

    if(m_sync_counter > m_queue_config.sync_rate )
    {
        //try after 200ms
        this->add_timer_after(&m_sync_timer,200) ;
        return ;
    }

    ServerHandler* handler = get_leader() ;
    if(handler)
    {
        SSSyncQueueRequest request ;
        request.head.seq = SYNC_TYPE_PULL ;
        request.body.set_last_trans_id(m_self_vote_info.trans_id()) ;

        handler->send(&request,0) ;
    }

}

const SyncQueueData& QueueServer::update_queue_log(SyncQueueData& sync_data)
{
    if(m_queue_log.size() > log_size() )
    {
        trace_log_format(m_logger,"queue_log full size:%d",m_queue_log.size()) ;
        for(int i=0;i<10 ;++i) m_queue_log.erase(m_queue_log.begin()) ;
    }

    int64_t trans_id = sync_data.trans_id() ;
    SyncQueueData& log_data = m_queue_log[trans_id] ;
    log_data.Swap(&sync_data) ;
    return log_data ;

}

void QueueServer::on_queue_log(SyncQueueData& sync_data)
{
    //write log
    int64_t last_trans_id = m_self_vote_info.trans_id() ;
    int64_t trans_id = last_trans_id +1  ;
    m_self_vote_info.set_trans_id(trans_id) ;

    sync_data.set_last_trans_id(last_trans_id) ;
    sync_data.set_trans_id(trans_id) ;
    const SyncQueueData& log_data = update_queue_log(sync_data) ;

    // broadcast to slaves
    if(is_leader() )
    {
        SSSyncQueueResponse notify ;
        notify.head.seq = SYNC_TYPE_PUSH ;
        notify.body.CopyFrom(log_data) ;
        std::set<int>::iterator it = m_push_sync_set.begin();
        while( it != m_push_sync_set.end() )
        {
            ServerHandler* handler = m_server_manager.get_server(*it) ;
            ++it ;

            handler->send(&notify,0) ;
        }
    }

}

int QueueServer::send_event(SyncQueueData* data)
{
    LocalEventData event_data ;
    event_data.type = SYNC_QUEUE_REQUEST ;
    event_data.timestamp = time(0) ;
    event_data.data = data ;

    int ret =  m_event_queue.push(event_data) ;

    if( ret !=0 ) return -1 ;

    m_event_handler.notify() ;
    return 0 ;
}

void QueueServer::on_event(int64_t v)
{
    LocalEventData event_data ;
    while( m_event_queue.pop(event_data) == 0 )
    {
        if(event_data.type == SYNC_QUEUE_REQUEST)
        {
            SyncQueueData* data = (SyncQueueData*)event_data.data;
            on_queue_log(*data) ;
            delete data ;
        }
    }
}



void QueueServer::server_info(Json::Value& info)
{
    info["log_size"] = (int)m_queue_log.size() ;
    info["max_log_size"] = get_app().log_size() ;
    info["leader_id"] = m_node_info.leader_id ;
    info["node_id"] = m_node_info.node_id ;
    info["trans_id"] = (Json::Int64)m_self_vote_info.trans_id() ;


}


IMPLEMENT_APPLICATION_INSTANCE(QueueServer) ;
IMPLEMENT_MAIN() ;


