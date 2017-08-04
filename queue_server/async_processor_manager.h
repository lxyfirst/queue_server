/*
 * async_processor_manager.h
 * Author: lxyfirst@163.com
 */

#pragma once

#include "framework/fsm_manager.h"
#include "framework/timer_manager.h"
#include "framework/packet.h"
#include "framework/tcp_data_handler.h"
#include "public/message.h"

enum
{
    STATUS_INIT = 0 ,
    STATUS_WAIT_DATA = 1 ,
    STATUS_WAIT_RESULT = 2 ,
};

enum
{
    PROCESSOR_DONE = -1 ,
    PROCESSOR_CONTINUE = 0 ,
};



class AsyncProcessorManager: public framework::fsm_manager
{
public:
    AsyncProcessorManager();
    virtual ~AsyncProcessorManager();

protected:
    virtual framework::base_fsm* alloc_fsm(int fsm_type);
    virtual void free_fsm(framework::base_fsm* object);
};



class MajorityProcessor : public framework::base_fsm , public framework::base_timer
{
public:
    MajorityProcessor():m_status(STATUS_INIT),m_wait_count(0),m_success_count(0) {  } ;
    virtual ~MajorityProcessor() { } ;

    int enter(framework::fsm_manager* fm,int event_type,void* arg);

    void on_timeout(framework::timer_manager* manager) ;

protected:
    void set_wait_count(int count) {m_wait_count = count ; } ;
    void set_success_count(int count) { m_success_count = count ; } ;
    void dec_wait_count() { --m_wait_count ; } ;
    void dec_success_count() { --m_success_count ; } ;

    int batch_request(framework::packet* p) ;

    virtual int init(void* data) =0 ;
    virtual int process(void* data) =0 ;
    virtual void on_timeout() {} ;
    virtual void on_result(bool success) = 0 ;

protected:

    int8_t m_status ;
    int8_t m_wait_count ;
    int8_t m_success_count ;


};

class ClientAsyncProcessor : public MajorityProcessor
{
public:
    virtual ~ClientAsyncProcessor() { } ;

    framework::tcp_data_handler::connection_id conn_id ;
};


class StartVoteProcessor : public ClientAsyncProcessor
{
public:
    enum {TYPE = SSVoteRequest::packet_type } ;
    ~StartVoteProcessor() ;
protected:
    virtual int init(void* data)  ;
    virtual int process(void* data) ;
    virtual void on_timeout() ;
    virtual void on_result(bool success) ;

public:
    SSVoteRequest request ;
    SSVoteResponse response ;

};



