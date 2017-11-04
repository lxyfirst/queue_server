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

VoteData& get_leader_vote_data(VoteData& data)
{
     data.CopyFrom(get_app().leader_vote_data()) ;
     return data ;
}

VoteData& get_self_vote_data(VoteData& data)
{
    data.CopyFrom(get_app().self_vote_data() ) ;
    return data ;
}

int max_queue_size() 
{
    return get_app().queue_size() ;
}

bool is_leader()
{
    return get_app().is_leader() ; 
}

bool is_forward_request()
{
    return get_app().is_forward_request() ;
}

int notify_sync_event(SyncQueueData* data)
{
    return get_app().send_event(data) ;
}

