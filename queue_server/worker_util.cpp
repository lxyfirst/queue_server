/**
 *
 *
 */

#include "worker_util.h"
#include "queue_server.h"

Worker& get_worker() 
{
    return get_app().get_worker() ;

}
log_thread& get_logger() 
{
    return get_worker().logger() ;
}

const VoteData* leader_vote_data() 
{
    return get_app().leader_vote_data() ;

}

const VoteData* self_vote_data() 
{
    return &get_app().self_vote_data() ;

}

int max_queue_size() 
{
    return get_app().queue_size() ;
}

bool is_leader()
{
    return get_app().is_leader() ; 
}

int notify_sync_event(SyncQueueData* data)
{
    return get_app().send_event(data) ;
}

const QueueNameContainer* real_queue_name(const std::string& name)
{
    return get_app().real_queue_name(name) ;
}
