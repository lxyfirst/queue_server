/*
 * worker.cpp
 *
 *  Created on: 2015Äê10ÔÂ30ÈÕ
 *      Author: dell
 */

#include "framework/system_util.h"
#include "framework/member_function_bind.h"
#include "worker.h"
#include "queue_processor.h"
#include "queue_server.h"


Worker::Worker(framework::log_thread& logger):m_logger(logger)
{


}

Worker::~Worker()
{

}


int Worker::on_init()
{
    if(m_reactor.init(10240)!=0) error_return(-1,"init reactor failed") ;
    m_timer_engine.init(time(0),10) ;
    if(m_event_queue.init(get_app().queue_size())!=0) error_return(-1,"init queue failed") ;
    eventfd_handler::callback_type callback = framework::member_function_bind(&Worker::on_event,this) ;
    if(m_event_handler.init(m_reactor,callback )!=0 )
    {
        error_return(-1,"init eventfd failed") ;
    }

    const VoteData& self_info = get_app().self_vote_data() ;
    const char* listen_host = self_info.host().c_str() ;
    int listen_port = self_info.port() ;
    if(m_udp_handler.init(&m_reactor,listen_host,listen_port)!=0 )
    {
        error_return(-1,"init udp failed");
    }

    framework::tcp_acceptor::callback_type client_callback = member_function_bind(&Worker::on_client_connection,this) ;
    if(m_client_acceptor.init(m_reactor,listen_host,listen_port ,client_callback )!=0)
    {
        error_return(-1,"init client acceptor failed") ;
    }

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

    if(client_handler->init(&m_reactor,fd) !=0)
    {
        m_client_pool.release(client_handler) ;
        return -1 ;
    }

    return 0 ;

}

void Worker::free_connection(ClientTcpHandler* client_handler)
{
    m_client_pool.release(client_handler) ;

}


void Worker::on_event(int64_t v)
{
    LocalEventData event_data ;
    while( m_event_queue.pop(event_data) == 0 )
    {
        switch(event_data.type)
        {
        case SYNC_QUEUE_REQUEST:
        {
            SyncQueueData* data = (SyncQueueData*)event_data.data;
            process_sync_queue(*data) ;
            delete data ;
            break ;
        }

        }


    }

}


void Worker::run_once()
{
    m_reactor.run_once(2000) ;
    m_timer_engine.run_until(time(0)) ;
}


int Worker::send_sync_request(const SyncQueueData& data)
{
    SyncQueueData* tmp = new SyncQueueData ;
    if(tmp == NULL) return -1 ;

    tmp->CopyFrom(data) ;
    if(send_event(SYNC_QUEUE_REQUEST,tmp)!=0)
    {
        delete tmp ;
        return -1 ;
    }

    return 0 ;

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


void Worker::process_sync_queue(SyncQueueData& sync_data)
{
    Queue* queue = get_queue(sync_data.queue()) ;
    if(queue) queue->update(sync_data) ;
}

void Worker::list_queue(Value& queue_list)
{
    QueueManager::iterator it = m_queue_manager.begin() ;
    for(; it!= m_queue_manager.end();++it)
    {
        if(it->second) queue_list[it->first] = it->second->size() ;
    }
}

