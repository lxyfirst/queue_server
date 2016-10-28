/*
 * Author: lxyfirst@163.com
 */

#pragma once

#include "worker.h"

Worker& get_worker() ;
log_thread& get_logger() ;

VoteData& get_leader_vote_data(VoteData& data);
VoteData& get_self_vote_data(VoteData& data);

int max_queue_size() ;
bool is_leader() ;

int notify_sync_event(SyncQueueData* data) ;





