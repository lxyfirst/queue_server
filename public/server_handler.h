/*
 * server_handler.h
 * Author: lixingyi
 */

#ifndef SERVER_HANDLER_H_
#define SERVER_HANDLER_H_

#include <stdint.h>
#include <tr1/functional>

#include "framework/tcp_data_handler.h"
//#include "framework/timer_manager.h"


class ServerManager ;

class ServerHandler: public framework::tcp_data_handler
{
public:
    enum {
        HEATBEAT_TIME = 10 ,
    } ;
public:
    ServerHandler(ServerManager* owner,int16_t local_id);
    virtual ~ServerHandler();

    int16_t local_server_id() const { return m_local_server_id ; } ;
    int16_t remote_server_id() const { return m_remote_server_id ; } ;

    //called every 10 seconds to check connection
    void check_connection() ;
    //void on_timeout(framework::timer_manager* manager) ;
protected:
    int get_packet_info(const char* data,int size,framework::packet_info* pi);

    int process_packet(const framework::packet_info* pi);

    void on_closed();

    void on_connected() ;

    void on_disconnect(int error_type) ;

    int process_register_request(const framework::packet_info* pi);
    int process_status_request(const framework::packet_info* pi);
private:
    //framework::template_timer<ServerHandler> m_timer ;
    ServerManager* m_owner ;
    int16_t  m_local_server_id ;
    int16_t  m_remote_server_id ;
    int m_last_time ;
};

#endif /* SERVER_HANDLER_H_ */
