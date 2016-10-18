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
#include "jsoncpp/json.h"
#include "framework/timer_manager.h"
#include "public/system.pb.h"

using std::string ;

struct QueueMessage
{
    QueueMessage():id(0),retry(0),delay(0),ttl(0) {} ;
    int id ;
    int retry ;
    int delay ;
    int ttl ;
    string data ;

};


typedef std::tr1::unordered_map<int,QueueMessage> MessageContainer ;
typedef std::multimap<int,int> QueueIndex ;
typedef std::pair<int,int> IndexData ;



class Queue
{
public:
    Queue(const string& name);
    virtual ~Queue();

    const string& name() const { return m_name ; } ;

    int size() const { return m_message.size() ; } ;
    int max_id() const { return m_seq_id ; } ;
    int wait_status() const ;
    /*
     * @brief 发布消息
     * @param request 消息对象
     * @return id or 0
     */
    int produce(const string& data,int delay,int ttl,int retry) ;

    /*
     * @brief 获取并移除可用消息，可用指在有效期内,若需要确认消息，放入重试队列
     * @param data存储返回的消息对象
     * @return id or 0
     */
    int consume(string& data);

    /*
     * @brief 删除消息
     */
    void erase(int id) ;

    void clear() ;

    void on_timeout(framework::timer_manager* manager) ;

    int update(const SyncQueueData& data) ;

private:
    Queue(const Queue& o) ;
    Queue& operator=(const Queue& o);

    QueueMessage* inner_produce(const string& data,int delay,int ttl,int retry,int id) ;

    void check_work_queue() ;

    QueueMessage* get_message(int id)
    {
        MessageContainer::iterator it = m_message.find(id) ;
        if(it == m_message.end()) return NULL ;
        return &it->second ;
    }

private:
    framework::template_timer<Queue> m_timer ;
    string m_name ;
    MessageContainer m_message ;
    QueueIndex m_work_queue ;
    QueueIndex m_retry_queue ;
    int m_seq_id ;
    int m_check_key ;

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

