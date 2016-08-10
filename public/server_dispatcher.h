/*
 * server_dispatcher.h
 * Author: lxyfirst@163.com
 */

#pragma once

#include <map>
#include <set>
#include <vector>
#include <string>

#include <stdint.h>


struct ServerInfo
{
    char host[24] ;
    int port ;
    int16_t node_id ;
    int8_t online_status ;
};


inline int16_t get_server_id(int8_t node_type,uint8_t node_id)
{
    return ((int16_t)node_type << 8) | node_id ;
}

inline int8_t get_node_id(int16_t server_id)
{
    return server_id & 0xFF ;
}

inline int8_t get_node_type(int16_t server_id)
{
    return (server_id >>8) & 0xFF ;
}

typedef std::map<int,ServerInfo> ServerInfoContainer ;
typedef std::set<int> ServerIdContainer ;

class DispatchAlgorithm
{
public:
    virtual ~DispatchAlgorithm() {} ;

    /*
     * @brief init data and  algorithm
     * @return 0 on success
     */
    virtual int on_init(const ServerInfoContainer& data) =0;

    /*
     * @brief dispatch by key
     * @return node_id
     */
    virtual int dispatch(int key) =0;
};

class ServerDispatcher
{
public:
    ServerDispatcher();
    virtual ~ServerDispatcher();

    /*
     * @brief get server info by key
     * @return node_id
     */
    int dispatch_by_key(int key) const;

    /*
     * @brief load config data and init dispatch algorithm
     * @return 0 on success
     */
    int load_config(const std::string& config_data,const ServerIdContainer& server_id_list) ;

    bool is_ready() const;

    const ServerInfoContainer* get_servers() const
    {
        return &m_server_list ;
    }

    const ServerInfo* get_server(int node_id) const
    {
        ServerInfoContainer::const_iterator it = m_server_list.find(node_id) ;
        if(it == m_server_list.end()) return NULL ;
        return &it->second ;
    }
private:
    /*
     * @brief create dispatch algorithm by name
     * @return algorithm pointer
     */
    DispatchAlgorithm* create_algorithm(const char* name) ;

    void reset() ;
    
private:
    ServerInfoContainer m_server_list ;
    DispatchAlgorithm* m_algorithm ;

};

class ConsistentHashAlgorithm : public DispatchAlgorithm
{
public:

    int on_init(const ServerInfoContainer& data) ;

    int dispatch(int key);
};

class HashAlgorithm : public DispatchAlgorithm
{
public:

    int on_init(const ServerInfoContainer& data) ;

    int dispatch(int key);
private:
    std::vector<int> m_node_list ;
};


