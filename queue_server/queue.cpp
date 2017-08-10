/*
 * channel.cpp
 * Author: lixingyi
 */

#include "framework/string_util.h"
#include "queue.h"
#include "worker_util.h"

#include "public/system.pb.h"


QueueMessage* QueueMessage::Create(int id,int retry,int ttl,int data_size,const char* data)
{
    QueueMessage* message = (QueueMessage*)malloc(sizeof(QueueMessage)+data_size+1) ;
    if(message == NULL) return NULL ;
    message->id = id ;
    message->retry = retry ;
    message->ttl = ttl ;
    message->data_size = data_size ;
    message->data[data_size] = 0 ;
    memcpy(message->data,data,data_size) ;
    return message ;
}

void QueueMessage::Destroy(QueueMessage* message)
{
    free(message) ;
}

Queue::Queue(const string& name):m_name(name),m_seq_id(0)
{


}

Queue::~Queue()
{
    clear() ;
}

int Queue::produce(const std::string& data,int delay,int ttl,int retry)
{

    if(m_message_container.size() > max_queue_size() )
    {
        warn_log_format(get_logger(),"queue full queue:%s delay:%d size:%d",
                m_name.c_str(),delay,data.size() ) ;
        return -1 ;
    }

    QueueMessage* message = inner_produce(data,delay,ttl,retry,0) ;
    if(message == NULL) return -1 ;
    if(message->data_size > 1024)
    {
        trace_log_format(get_logger(),"produce message queue:%s id:%d delay:%d size:%d",
            m_name.c_str(),message->id,delay,message->data_size ) ;
    }
    else
    {
        trace_log_format(get_logger(),"produce message queue:%s id:%d delay:%d data:%s",
            m_name.c_str(),message->id,delay,message->data ) ;
    }
    //notify
    SyncQueueData* sync_data  = new SyncQueueData;
    sync_data->set_op_type(PUSH_QUEUE_REQUEST) ;
    sync_data->set_message_id(message->id) ;
    sync_data->set_queue(m_name) ;

    sync_data->set_delay(delay) ;
    sync_data->set_ttl(ttl) ;
    sync_data->set_retry(retry) ;

    sync_data->set_data(message->data,message->data_size);

    if( notify_sync_event(sync_data)!=0) delete sync_data ;

    return message->id ;

}

QueueMessage* Queue::inner_produce(const string& data,int delay,int ttl,int retry,int id)
{


    if(id < 1 )
    {
        id = create_message_id(m_seq_id) ;
    }
    else
    {
        m_seq_id = id ;
    }

    QueueMessage* message = QueueMessage::Create(id,retry,ttl,data.size(),data.data());
    if(message == NULL) return NULL ;

    MessageValue value(delay,message) ;
    m_id_container[id] = m_message_container.insert(value) ;

    return message ;

}


int Queue::consume(string& data)
{
    if( m_message_container.size() <1 ) return 0 ;

    int now = time(0) ;
    MessageContainer::iterator it = m_message_container.begin() ;
    int delay = it->first  ;
    if( delay > now) return 0 ;

    QueueMessage* message = it->second ;

    data.assign(message->data,message->data_size) ;

    int msg_id = message->id ;
    int retry_time = now + message->retry ;
    if(message->retry >0 && retry_time < message->ttl)
    {
        MessageValue value(retry_time ,message) ;
        m_message_container.erase(it) ;
        m_id_container[msg_id] = m_message_container.insert(value) ;

    }
    else
    {
        erase(msg_id) ;
    }

    return msg_id ;

}

void Queue::inner_erase(int msg_id)
{
    IdContainer::iterator it = m_id_container.find(msg_id) ;
    if(it != m_id_container.end())
    {
        MessageContainer::iterator message_it = it->second ;
        QueueMessage::Destroy(message_it->second) ;
        m_message_container.erase(message_it) ;
        m_id_container.erase(it) ;
    }
}

void Queue::erase(int msg_id)
{
    inner_erase(msg_id) ;
    trace_log_format(get_logger(),"erase message queue:%s id:%d",m_name.c_str(),msg_id) ;

    //notify
    SyncQueueData* sync_data = new SyncQueueData;
    sync_data->set_queue(m_name) ;
    sync_data->set_op_type(POP_QUEUE_REQUEST) ;
    sync_data->set_message_id(msg_id) ;

    if(notify_sync_event(sync_data)!=0) delete sync_data ;

}



int Queue::update(const SyncQueueData& sync_data)
{
    int id = sync_data.message_id();

    if(sync_data.op_type() == POP_QUEUE_REQUEST )
    {
        trace_log_format(get_logger(),"sync erase message queue:%s id:%d",m_name.c_str(),id ) ;
        inner_erase(id) ;
    }
    else if ( sync_data.op_type() == PUSH_QUEUE_REQUEST )
    {
        trace_log_format(get_logger(),"sync produce message queue:%s id:%d size:%d",
                m_name.c_str(),id, sync_data.data().size() ) ;

        //todo queue is full and update
        int over_size = m_message_container.size() - max_queue_size();
        if(over_size >0 && over_size %10 == 1 )
        {
            warn_log_format(get_logger(),"update full queue:%s size:%d",
                    m_name.c_str(),m_message_container.size() ) ;
        }

        inner_produce(sync_data.data(),sync_data.delay(),sync_data.ttl(),sync_data.retry(),id);
    }

    return 0 ;
}

void Queue::clear()
{
    for(auto& pair : m_message_container)
    {
        QueueMessage::Destroy(pair.second) ;
    }

    m_message_container.clear() ;
    m_id_container.clear() ;

}


int Queue::wait_status() const
{
    int now = time(NULL) , count = 0 ,total_time =0 ;
    for(auto& pair : m_message_container)
    {
        int delay = pair.first ;
        if( (count & 0x10) || (delay > now) ) break ;
        total_time += (now - pair.first) & 0xFF ;
        ++count ;

    }

    return total_time ;

}


Queue* QueueManager::create_queue(const string& queue_name)
{
    if (m_queue_list.count(queue_name) >0 ) return NULL ;

    Queue* queue = new Queue(queue_name) ;
    if(queue == NULL) return NULL ;

    m_queue_list[queue_name] = queue ;

    return queue ;
}


QueueManager::~QueueManager()
{
    for(auto& pair : m_queue_list)
    {
        delete pair.second ;
    }

    m_queue_list.clear() ;

}


