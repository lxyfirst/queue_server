/*
 * async_processor_manager.cpp
 * Author: lixingyi
 */

#include "async_processor_manager.h"
//#include "user_async_processor.h"
#include "queue_server.h"

using namespace framework ;


AsyncProcessorManager::AsyncProcessorManager()
{
    // TODO Auto-generated constructor stub

}

AsyncProcessorManager::~AsyncProcessorManager()
{
    clear() ;
}

base_fsm* AsyncProcessorManager::alloc_fsm(int fsm_type)
{

    switch(fsm_type)
    {
    case StartVoteProcessor::TYPE:
        return new StartVoteProcessor ;
    default:
        ;
    }


    return NULL ;
}

void AsyncProcessorManager::free_fsm(base_fsm* object)
{
    delete object ;
}


int MajorityProcessor::enter(fsm_manager* fm,int event_type,void* arg)
{

    if(m_status == STATUS_INIT && init(arg)== PROCESSOR_CONTINUE )
    {
        m_status = STATUS_WAIT_DATA ;
        arg = NULL ;

        get_app().add_timer_after(this,5000) ;

        if( m_wait_count >0  || m_success_count >0) return 0 ;

    }

    if(m_status == STATUS_WAIT_DATA && process(arg) == PROCESSOR_CONTINUE )
    {
        if( m_wait_count >0 && m_success_count > 0) return 0 ;

        m_status = STATUS_WAIT_RESULT ;
        arg = NULL ;
    }

    if(m_status == STATUS_WAIT_RESULT)
    {
        on_result(m_success_count == 0) ;
    }

    fm->destroy_fsm(this) ;

    return 0 ;

}

int MajorityProcessor::batch_request(framework::packet* p)
{
    int send_count = get_app().broadcast(p) ;
    if(send_count >0)
    {
        set_wait_count(send_count) ;
        set_success_count(get_app().majority_count() ) ;
        return PROCESSOR_CONTINUE ;
    }

    return PROCESSOR_DONE ;
}


void MajorityProcessor::on_timeout(timer_manager* manager)
{

    info_log_format(get_app().logger(),"fsm timeout fsm_id:%d",this->get_id());

    //process(NULL) ;
    on_timeout() ;

    get_app().async_manager().destroy_fsm(this->get_id()) ;
}


int StartVoteProcessor::init(void* data)
{
    VoteData* body = (VoteData*)data ;
    trace_log_format(get_app().logger(),"broadcast vote vote_id:%d node_id:%d",body->vote_id(),body->node_id() ) ;

    request.head.seq = this->get_id() ;
    request.head.src_key = request.head.dst_key = body->node_id() ;
    request.body.CopyFrom(*body) ;

    return batch_request(&request) ;

}


int StartVoteProcessor::process(void* data)
{
    packet_info* pi = (packet_info*)data ;
    if(response.decode(pi->data,pi->size)!=pi->size) return PROCESSOR_DONE  ;

    trace_log_format(get_app().logger(),"vote response node_id:%d result:%d",
            response.head.src_key ,response.body.error_code()) ;

    dec_wait_count() ;
    if(response.body.error_code() == EC_SUCCESS )
    {
        dec_success_count() ;
    }
    else if(response.body.error_code() == EC_VOTE_LEADER_EXIST)
    {
        get_app().set_leader(response.body.data()) ;
        return PROCESSOR_DONE;
    }
    else
    {
        return PROCESSOR_DONE ;
    }

    return PROCESSOR_CONTINUE ;

}


void StartVoteProcessor::on_result(bool success)
{
    if(success)
    {
        info_log_format(get_app().logger(),"vote success") ;
        SSVoteNotify notify;
        notify.head.seq = this->get_id() ;
        notify.body.CopyFrom(request.body) ;
        get_app().broadcast(&notify) ;
        get_app().set_leader(request.body) ;

    }
    else
    {
        info_log_format(get_app().logger(),"vote failed") ;
    }

}

void StartVoteProcessor::on_timeout()
{
    info_log_format(get_app().logger(),"vote timeout") ;
}

StartVoteProcessor::~StartVoteProcessor()
{
    get_app().stop_vote() ;
}

