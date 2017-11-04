/*
 * queue.h
 * Author: lxyfirst@163.com
 */

#pragma once

#include <string>
#include <stdint.h>
#include <map>
#include <list>
#include <tr1/unordered_map>

using std::string ;

struct QueueMessage
{
    static QueueMessage* Create(int id,int retry,int ttl,int data_size,const char* data);

    static void Destroy(QueueMessage* message);

    int id ;
    int ttl ;
    int retry ;
    int data_size ;
    char data[0] ;
};

inline int32_t create_message_id(int32_t& current_id)
{
    current_id = ( current_id & 0xfffffff ) +1 ;
    return current_id ;
}

typedef std::multimap<int,QueueMessage*> MessageContainer ;
typedef MessageContainer::value_type MessageValue ;
typedef std::tr1::unordered_map<int,MessageContainer::iterator> IdContainer ;

class SyncQueueData ;

class Queue
{
public:
    Queue(const string& name);
    virtual ~Queue();

    const string& name() const { return m_name ; } ;

    int size() const { return m_message_container.size() ; } ;
    int max_id() const { return m_seq_id ; } ;

    /*
     * @brief 队列消息被延期消费的时间统计
     */
    int wait_status() const ;

    /*
     * @brief 发布消息
     * @param request 消息对象
     * @return id or 0
     */
    int produce(const string& data,int delay,int ttl,int retry) ;

    /*
     * @brief 消费消息
     * @param data存储返回的消息对象
     * @return id or 0
     */
    int consume(string& data);

    /*
     * @brief 删除消息
     */
    void erase(int msg_id) ;

    void clear() ;

    int update(const SyncQueueData& data) ;

private:
    Queue(const Queue& o) ;
    Queue& operator=(const Queue& o);

    QueueMessage* inner_produce(const string& data,int delay,int ttl,int retry,int id) ;

    void inner_erase(int msg_id) ;

private:

    string m_name ;
    MessageContainer m_message_container ;
    IdContainer m_id_container ;
    int32_t m_seq_id ;


};

class QueueManager
{
public:
    typedef std::tr1::unordered_map<string,Queue*> QueueContainer ;
    typedef QueueContainer::const_iterator iterator ;
public:
    bool exist(const string& queue_name) const
    {
        return m_queue_list.count(queue_name) >0 ;
    }

    Queue* get_queue(const string& queue_name)
    {
        QueueContainer::iterator it = m_queue_list.find(queue_name) ;
        if(it == m_queue_list.end()) return NULL ;
        return it->second ;
    }

    Queue* create_queue(const string& queue_name) ;


    iterator begin() const { return m_queue_list.begin() ;} ;
    iterator end() const { return m_queue_list.end() ; } ;
    int size() const { return m_queue_list.size() ; } ;
    ~QueueManager() ;

private:
    QueueContainer m_queue_list ;

};

