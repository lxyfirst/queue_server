/*
 * worker.cpp
 *
 *  Created on: 2015��10��30��
 *      Author: dell
 */

#include "framework/system_util.h"
#include "worker.h"
#include "queue_processor.h"
#include "worker_util.h"
#include "public/message.h"
#include "rapidjson/writer.h"

using namespace framework ;

int json_decode(const char* begin,const char* end,Document& request)
{
    if(request.Parse(begin,end-begin).HasParseError()) return -1 ;

    if(!request.IsObject()) return -2 ;

    return 0 ;

}

bool json_encode(const Value& json,StringBuffer& buffer)
{
    rapidjson::Writer<StringBuffer> writer(buffer) ;
    return json.Accept(writer) ;
}

bool json_check_field(const Value&json,const JsonFieldInfo& field_list)
{
    if(!json.IsObject()) return false ;

    int matched = 0 ;
    for(Value::ConstMemberIterator m = json.MemberBegin(); m != json.MemberEnd(); ++m)
    {
        JsonFieldInfo::const_iterator it = field_list.find(m->name.GetString()) ;
        if(it == field_list.end()) continue ;
        switch(it->second)
        {
        case rapidjson::kObjectType :
            if(!m->value.IsObject()) return false ;
            break ;
        case rapidjson::kArrayType :
            if(!m->value.IsArray()) return false ;
            break ;
        case rapidjson::kNumberType :
            if(!m->value.IsNumber()) return false ;
            break ;
        case rapidjson::kStringType :
            if(!m->value.IsString()) return false ;
            break ;

        }

        matched+=1 ;
        if(matched == field_list.size() ) return true ;


    }

    return false ;

    /*
    for(auto& field : field_list)
    {
        if(!json.HasMember(field.first)) return false ;
        switch(field.second)
        {
        case rapidjson::kObjectType :
            if(!json[field.first].IsObject()) return false ;
            break ;
        case rapidjson::kArrayType :
            if(!json[field.first].IsArray()) return false ;
            break ;
        case rapidjson::kNumberType :
            if(!json[field.first].IsNumber()) return false ;
            break ;
        case rapidjson::kStringType :
            if(!json[field.first].IsString()) return false ;
            break ;

        }

    }
    return true ;
    */


}


int json_get_value(const Value& json,const char* key,int default_value)
{
    if(!json.IsObject() ) return default_value ;
    Value::ConstMemberIterator it = json.FindMember(key) ;
    if(it == json.MemberEnd() || !it->value.IsInt() ) return default_value ;
    return it->value.GetInt() ;
}

const char* json_get_value(const Value& json,const char* key,const char* default_value)
{
    if(!json.IsObject() ) return default_value ;
    Value::ConstMemberIterator it = json.FindMember(key) ;
    if(it == json.MemberEnd() || !it->value.IsString() ) return default_value ;
    return it->value.GetString() ;
}


Worker::Worker(framework::log_thread& logger):m_logger(logger)
{
    m_timer.set_callback(this,&Worker::on_timeout) ;
}

Worker::~Worker()
{

}

int Worker::init(VirtualQueueContainer& virtual_queue)
{
    if(this->is_run())
    {
        notify_queue_config(virtual_queue) ;
    }
    else
    {
        m_virtual_queue.swap(virtual_queue) ;
    }

    return 0 ;

}


int Worker::on_init()
{
    if(m_reactor.init(10240)!=0) error_return(-1,"init reactor failed") ;
    m_timer_engine.init(time(0),10) ;
    if(m_event_queue.init(max_queue_size())!=0) error_return(-1,"init queue failed") ;
    eventfd_handler::callback_type callback = std::bind(&Worker::on_event,this,std::placeholders::_1) ;
    if(m_event_handler.init(m_reactor,callback )!=0 )
    {
        error_return(-1,"init eventfd failed") ;
    }

    VoteData self_info ;
    get_self_vote_data(self_info) ;

    const char* listen_host = self_info.host().c_str() ;
    int listen_port = self_info.port() ;
    if(m_udp_handler.init(m_reactor,listen_host,listen_port)!=0 )
    {
        error_return(-1,"init udp failed");
    }

    using std::placeholders::_1 ;
    using std::placeholders::_2 ;
    framework::tcp_acceptor::callback_type client_callback = std::bind(&Worker::on_client_connection,this,_1,_2) ;
    if(m_client_acceptor.init(m_reactor,listen_host,listen_port ,client_callback )!=0)
    {
        error_return(-1,"init client acceptor failed") ;
    }

    on_timeout(NULL) ;

    return 0;
}

void Worker::on_fini()
{
    m_udp_handler.fini() ;
    on_event(1) ;
    m_event_handler.fini() ;
    m_reactor.fini() ;

}

int Worker::on_client_connection(int fd,sa_in_t* addr)
{
    ClientTcpHandler* client_handler = m_client_pool.create() ;

    if(client_handler == NULL) return -1 ;

    if(client_handler->init(m_reactor,fd) !=0)
    {
        m_client_pool.release(client_handler) ;
        return -1 ;
    }

    return 0 ;

}

void Worker::on_client_closed(ClientTcpHandler* client_handler)
{
    if(client_handler != &m_leader_handler)
    {
        free_connection(client_handler);
    }
}

void Worker::free_connection(ClientTcpHandler* client_handler)
{
    m_client_pool.release(client_handler) ;

}


void Worker::on_timeout(framework::timer_manager* manager)
{
    add_timer_after(&m_timer,(ClientTcpHandler::IDLE_TIMEOUT >>1)) ;

    if(m_leader_handler.is_closed())
    {
        this->init_leader_handler() ;
    }
    else
    {
        m_leader_handler.send_heartbeat() ;
    }


}

void Worker::on_event(int64_t v)
{
    LocalEventData event_data ;
    while( m_event_queue.pop(event_data) == 0 )
    {
        switch(event_data.type)
        {
        case SYNC_QUEUE_REQUEST:
            on_sync_request(event_data.data);
            break ;
        case VOTE_NOTIFY:
            on_leader_change(event_data.data) ;
            break ;
        case CONFIG_DATA_REQUEST:
            on_queue_config(event_data.data) ;
            break ;
        }


    }

}


void Worker::run_once()
{
    m_reactor.run_once(2000) ;
    m_timer_engine.run_until(time(0)) ;
}


int Worker::notify_sync_request(const SyncQueueData& data)
{
    SyncQueueData* tmp = new SyncQueueData ;
    if(tmp == NULL)
    {
        error_log_format(m_logger,"invalid sync queue event") ;
        return -1 ;
    }

    tmp->CopyFrom(data) ;
    if(send_event(SYNC_QUEUE_REQUEST,tmp)!=0)
    {
        error_log_format(m_logger,"sync queue failed") ;
        delete tmp ;
        return -1 ;
    }

    return 0 ;

}

int Worker::notify_queue_config(VirtualQueueContainer& virtual_queue)
{
    VirtualQueueContainer* data = new VirtualQueueContainer ;
    if(data == NULL)
    {
        error_log_format(m_logger,"sync config failed") ;
        return -1 ;
    }
    data->swap(virtual_queue) ;

    if(send_event(CONFIG_DATA_REQUEST,data)!=0)
    {
        error_log_format(m_logger,"sync config failed") ;
        delete data ;
        return -1 ;
    }
    return 0 ;
}

int Worker::notify_leader_change()
{
    return send_event(VOTE_NOTIFY,NULL) ;
}

void Worker::on_sync_request(void* data)
{
    SyncQueueData* sync_data = static_cast<SyncQueueData*>(data) ;
    if(sync_data == NULL)
    {
        error_log_format(m_logger,"sync request failed") ;
        return  ;
    }

    process_sync_queue(*sync_data) ;
    delete sync_data ;

}

void Worker::on_queue_config(void* data)
{
    VirtualQueueContainer* virtual_queue = static_cast<VirtualQueueContainer*>(data) ;
    if(virtual_queue == NULL)
    {
        error_log_format(m_logger,"sync config failed") ;
    }
    else
    {
        m_virtual_queue.swap(*virtual_queue) ;
        delete virtual_queue ;
        info_log_format(m_logger,"sync config success") ;
    }
}

void Worker::on_leader_change(void* data)
{
	init_leader_handler() ;
}

int Worker::init_leader_handler()
{
    m_leader_handler.fini() ;

    if(is_leader() ) return -1;

    VoteData leader_info ;
    get_leader_vote_data(leader_info);

    if(leader_info.node_id() <1 || leader_info.host().size() < 1 || leader_info.port() < 1 )
    {
    	return -1;
    }

    info_log_format(m_logger,"try connect to leader node_id:%d host:%s",
            leader_info.node_id(),leader_info.host().c_str() );

    return m_leader_handler.init(m_reactor,leader_info.host().c_str(),leader_info.port() );

}

int Worker::send_event(int type,void* data)
{
    LocalEventData event_data ;
    event_data.type = type ;
    event_data.timestamp = time(0) ;
    event_data.data = data ;

    int ret =  m_event_queue.push(event_data) ;

    if( ret !=0 ) return -1 ;

    m_event_handler.notify() ;
    return 0 ;
}

int Worker::add_timer_after(framework::base_timer* timer,int seconds)
{
    if(seconds <1  ) return -1 ;
    timer->set_expired(time(0)+seconds) ;
    return m_timer_engine.add_timer(timer) ;
}

void Worker::del_timer(framework::base_timer* timer)
{
    m_timer_engine.del_timer(timer) ;
}

Queue* Worker::get_queue(const string& queue_name)
{
    Queue* queue = m_queue_manager.get_queue(queue_name);
    if(queue == NULL )
    {
        queue = m_queue_manager.create_queue(queue_name) ;
        info_log_format(m_logger,"auto create  queue :%s",queue_name.c_str()) ;
    }

    return queue ;
}

const QueueNameContainer* Worker::real_queue_name(const std::string& virtual_name)
{
    VirtualQueueContainer::const_iterator it = m_virtual_queue.find(virtual_name);
    if( it == m_virtual_queue.end() ) return NULL ;
    return &it->second ;
}


void Worker::process_sync_queue(SyncQueueData& sync_data)
{
    Queue* queue = get_queue(sync_data.queue()) ;
    if(queue) queue->update(sync_data) ;
}

void Worker::list_queue(Document& queue_list)
{
    queue_list.SetArray() ;
    for(auto& pair : m_queue_manager)
    {
        if(pair.second == NULL) continue ;

        Value value  ;
        value.SetObject() ;
        value.AddMember("size",pair.second->size(),queue_list.GetAllocator() ) ;
        value.AddMember("wait_status",pair.second->wait_status(),queue_list.GetAllocator() ) ;
        value.AddMember("name",pair.first,queue_list.GetAllocator()) ;
        queue_list.PushBack(value,queue_list.GetAllocator() ) ;
    }
}

int Worker::process_forward_request(ClientTcpHandler* handler,const packet_info* pi)
{
    if(!is_leader() ) return -1 ;
    SSForwardRequest forward ;
    if(forward.decode(pi->data,pi->size)!= pi->size) return -1 ;

    Document request ;
    const char* begin =forward.body.data().c_str() ;
    const char* end = begin + forward.body.data().length() ;
    if(json_decode(begin,end,request)!=0) return -1 ;

    if(QueueProcessor::process(request) !=0) return -1 ;

    StringBuffer buffer ;
    json_encode(request,buffer) ;

    SSForwardResponse response ;
    response.body.Swap(&forward.body) ;
    response.body.set_data( buffer.GetString(),buffer.GetSize() ) ;
    return handler->send(&response,0) ;

}

int Worker::process_forward_response(ClientTcpHandler* handler, const packet_info* pi)
{
    SSForwardResponse forward ;
    if(forward.decode(pi->data,pi->size)!= pi->size) return -1 ;

    if(time(0) - forward.body.timestamp() < 30 &&
            sizeof(SourceData) == forward.body.source().length() )
    {
        SourceData* source = (SourceData*)forward.body.source().data() ;
        const std::string& data = forward.body.data() ;
        if(source->is_tcp)
        {
            ClientTcpHandler* client = dynamic_cast<ClientTcpHandler*>(m_reactor.get_handler(source->id.fd) ) ;
            if(client && source->id == client->get_id() )
            {
                client->send(data.c_str(),data.size(),0) ;
                return 0 ;
            }

        }
        else
        {
            m_udp_handler.send(&source->addr,data.c_str(),data.size()) ;
            return 0 ;
        }
    }

    trace_log_format(m_logger,"drop response") ;

    return 0 ;
}

int Worker::forward_to_leader(const SourceData& source,const char* data,int size)
{
    SSForwardRequest forward ;
    forward.body.set_timestamp(time(0)) ;
    forward.body.set_data(data,size);

    forward.body.set_source((const char*)&source,sizeof(source)) ;

    return m_leader_handler.send(&forward,0);

}

