/*
 * Author: lxyfirst@163.com
 */

#pragma once
#include <string>
#include <vector>
#include <tr1/unordered_map>

#include "worker.h"
#include "public/system.pb.h"

typedef std::vector<std::string> QueueNameContainer ;
typedef std::tr1::unordered_map<std::string,QueueNameContainer > VirtualQueueContainer ;

Worker& get_worker() ;
log_thread& get_logger() ;
const VoteData* leader_vote_data() ;
const VoteData* self_vote_data() ;
int max_queue_size() ;
bool is_leader() ;
int notify_sync_event(SyncQueueData* data) ;
const QueueNameContainer* real_queue_name(const std::string& name) ;




