/*
 * channel.cpp
 * Author: lixingyi
 */

#include "framework/string_util.h"
#include "queue.h"
#include "queue_server.h"

Queue::Queue(const string& name):m_name(name),m_seq_id(0),m_check_key(0)
{
    m_timer.set_owner(this) ;

}

Queue::~Queue()
{
    // TODO Auto-generated destructor stub
}

int Queue::produce(const std::string& data,int delay,int ttl,int retry)
{

    if(m_message.size() > get_app().queue_size() )
    {
        warn_log_format(get_app().get_worker().logger(),"queue full queue:%s delay:%d size:%d",
                m_name.c_str(),delay,data.size() ) ;
        return -1 ;
    }

    QueueMessage* message = inner_produce(data,delay,ttl,retry,0) ;
    if(message == NULL) return -1 ;
    if(message->data.size() > 1024)
    {
        trace_log_format(get_app().get_worker().logger(),"produce message queue:%s id:%d size:%d",
            m_name.c_str(),message->id,message->data.size() ) ;
    }
    else
    {
        trace_log_format(get_app().get_worker().logger(),"produce message queue:%s id:%d data:%s",
            m_name.c_str(),message->id,message->data.c_str() ) ;
    }
    //notify
    SyncQueueData* sync_data  = new SyncQueueData;
    sync_data->set_op_type(PUSH_QUEUE_REQUEST) ;
    sync_data->set_message_id(message->id) ;
    sync_data->set_queue(m_name) ;

    sync_data->set_delay(message->delay) ;
    sync_data->set_ttl(message->ttl) ;
    sync_data->set_retry(message->retry) ;

    sync_data->set_data(message->data);

    if(get_app().send_event(sync_data)!=0) delete sync_data ;

    return message->id ;

}

QueueMessage* Queue::inner_produce(const string& data,int delay,int ttl,int retry,int id)
{

    if(id < 1 )
    {
        m_seq_id = ( m_seq_id & 0xfffffff ) +1 ;
        id = m_seq_id ;
    }
    else
    {
        m_seq_id = id ;
    }

    QueueMessage& queue_message = m_message[id]  ;
    queue_message.id = id ;
    queue_message.delay = delay ;
    queue_message.ttl = ttl ;
    queue_message.retry = retry ;
    queue_message.data = data ;

    m_work_queue.insert(IndexData(delay,id)) ;

    return &queue_message ;
}


int Queue::consume(string& data)
{
    if( m_work_queue.size() <1 ) return 0 ;

    int now = time(0) ;
    QueueIndex::iterator it;
    while(( it = m_work_queue.begin()) != m_work_queue.end() )
    {
        if(it->first > now ) return 0 ;
        QueueMessage* message = get_message(it->second) ;
        m_work_queue.erase(it) ;

        if ( message == NULL) continue ;

        int msg_id = message->id ;
        trace_log_format(get_app().get_worker().logger(),"consume message queue:%s id:%d",m_name.c_str(),msg_id) ;

        int retry_time = now + message->retry ;
        if(message->retry >0 && retry_time < message->ttl)
        {
            data = message->data ;
            m_retry_queue.insert(IndexData(retry_time ,message->id)) ;
        }
        else
        {
            data.swap(message->data) ;
            erase(message->id) ;
        }

        return msg_id ;

    }

    return 0 ;

}

void Queue::erase(int id)
{
    if(m_message.count(id) == 0 ) return ;
    m_message.erase(id) ;
    trace_log_format(get_app().get_worker().logger(),"erase message queue:%s id:%d",m_name.c_str(),id) ;

    //notify
    SyncQueueData* sync_data = new SyncQueueData;
    sync_data->set_queue(m_name) ;
    sync_data->set_op_type(POP_QUEUE_REQUEST) ;
    sync_data->set_message_id(id) ;

    if(get_app().send_event(sync_data)!=0) delete sync_data ;

}

int Queue::update(const SyncQueueData& sync_data)
{
    int id = sync_data.message_id();

    if(sync_data.op_type() == POP_QUEUE_REQUEST )
    {
        trace_log_format(get_app().get_worker().logger(),"sync erase message queue:%s id:%d",m_name.c_str(),id ) ;
        m_message.erase(id) ;
    }
    else if ( sync_data.op_type() == PUSH_QUEUE_REQUEST )
    {
        trace_log_format(get_app().get_worker().logger(),"sync produce message queue:%s id:%d size:%d",
                m_name.c_str(),id, sync_data.data().size() ) ;

        //todo queue is full and update
        int over_size = m_message.size() - get_app().queue_size();
        if(over_size >0 && over_size %10 == 1 )
        {
            warn_log_format(get_app().get_worker().logger(),"update full queue:%s size:%d",
                    m_name.c_str(),m_message.size() ) ;
        }

        inner_produce(sync_data.data(),sync_data.delay(),sync_data.ttl(),sync_data.retry(),id);
    }

    return 0 ;
}

void Queue::clear()
{
    m_message.clear() ;
    m_work_queue.clear() ;
    m_retry_queue.clear() ;
}

void Queue::check_work_queue()
{
    int check_count = 100 ;
    QueueIndex::iterator it = m_work_queue.lower_bound(m_check_key) ;
    while((it!=m_work_queue.end()) && (--check_count >0) )
    {
        if(m_message.count(it->second)==0)
        {
            QueueIndex::iterator erase_it = it ;
            ++it ;
            m_work_queue.erase(erase_it) ;
        }
        else
        {
            ++it ;
        }
    }

    if(check_count >0 ) m_check_key = 0 ;
    else m_check_key = it->first ;
}


void Queue::on_timeout(framework::timer_manager* manager)
{

    int now = time(0) ;

    //move from retry queue to work queue
    QueueIndex::iterator it = m_retry_queue.begin();
    for(; (it!=m_retry_queue.end()) && (it->first <= now ) ;++it )
    {
        QueueMessage* message = get_message(it->second) ;
        if(message )
        {
            m_work_queue.insert(IndexData(it->first,it->second));
            debug_log_format(get_app().get_worker().logger(),"retry message queue:%s id:%d",m_name.c_str(),it->second) ;
        }
    }

    m_retry_queue.erase(m_retry_queue.begin(),it) ;

    check_work_queue() ;

    //todo remove expired message

    get_app().add_timer_after(&m_timer,1000) ;


}

Queue* QueueManager::create_queue(const string& queue_name)
{
    if (m_queue_list.count(queue_name) >0 ) return NULL ;

    Queue* queue = new Queue(queue_name) ;
    if(queue == NULL) return NULL ;

    m_queue_list[queue_name] = queue ;
    queue->on_timeout(NULL) ;

    return queue ;
}


QueueManager::~QueueManager()
{
    for(QueueContainer::iterator it=m_queue_list.begin();it!=m_queue_list.end();++it)
    {
        delete it->second ;
    }

    m_queue_list.clear() ;

}


