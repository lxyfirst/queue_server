/*
 * client_udp_handler.cpp
 * Author: lixingyi
 */



#include "client_udp_handler.h"
#include "worker_util.h"
#include "queue_processor.h"

ClientUdpHandler::ClientUdpHandler()
{
    // TODO Auto-generated constructor stub

}

ClientUdpHandler::~ClientUdpHandler()
{
    // TODO Auto-generated destructor stub
}



int ClientUdpHandler::process_packet(const udp_packet* p)
{
    if(p->data[0] != '{') return 0 ;

    Document request ;
    if(json_parse_request(p->data,p->data + p->data_size,request)!=0) return 0 ;

    char remote_host[16] = {0} ;
    framework::addr2str(remote_host,sizeof(remote_host),&p->addr) ;

    debug_log_format(get_logger(),"recv host:%s data:%s",remote_host,p->data) ;

    int action = request[FIELD_ACTION].GetInt() ;
    if((!is_leader() ) && action < ACTION_LOCAL_START)
    {
        SourceData source ;
        source.is_tcp = 0 ;
        source.addr = p->addr ;
        if(get_worker().forward_to_leader(source,p->data,p->data_size)!=0 )
        {
        	this->send(&p->addr,"{}",2) ;
        }

        return 0 ;
    }

    if( QueueProcessor::process(request) ==0)
    {
        rapidjson::StringBuffer buffer ;
        json_encode(request,buffer) ;
        this->send(&p->addr,buffer.GetString(),buffer.GetSize() ) ;
    }

    return 0 ;


}





