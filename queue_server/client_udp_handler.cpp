/*
 * client_udp_handler.cpp
 * Author: lixingyi
 */



#include "client_udp_handler.h"
#include "queue_server.h"
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

    Json::Value request ;
    if(parse_request(p->data,p->data + p->data_size,request)!=0) return 0 ;

    char remote_host[16] = {0} ;
    framework::addr2str(remote_host,sizeof(remote_host),&p->addr) ;

    debug_log_format(get_app().get_worker().logger(),"recv host:%s data:%s",remote_host,p->data) ;

    if( QueueProcessor::process(request) ==0)
    {
        Json::FastWriter writer ;
        std::string data = writer.write(request) ;
        this->send(&p->addr,data.data(),data.size() ) ;
    }

    return 0 ;


}





