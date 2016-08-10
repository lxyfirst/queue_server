/*
 * template_packet.h
 *
 *  Created on: 2012-4-1
 *      Author: lxyfirst@163.com
 */

#pragma once

#include <sstream>
#include <exception>
#include "google/protobuf/message.h"

#include "framework/packet.h"
#include "framework/aliased_streambuf.h"


struct PacketHead
{
    uint16_t length ;
    uint16_t msg_type ;
} __attribute__((packed)) ;

union MsgType
{
    uint16_t value ;
    struct
    {
        uint16_t module_type:8 ;
        uint16_t action_type:8 ;

    } __attribute__((packed)) ;
} __attribute__((packed)) ;


inline int16_t decode_packet_size(const char* data)
{
    return ntoh_int16(((PacketHead*)data)->length) ;
}

inline int16_t decode_packet_msg_type(const char* data)
{
    return ntoh_int16(((PacketHead*)data)->msg_type) ;
}

inline void decode_packet_head(PacketHead* head,const char* data)
{
    head->length = ntoh_int16( ((PacketHead*)data)->length ) ;
    head->msg_type = ntoh_int16( ((PacketHead*)data)->msg_type ) ;
}

inline void encode_packet_head(char* data,const PacketHead* head)
{
    ((PacketHead*)data)->length = hton_int16(head->length) ;
    ((PacketHead*)data)->msg_type = hton_int16(head->msg_type) ;
}



struct SSHead
{
    uint32_t seq ;
    uint32_t dst_key ;
    uint32_t src_key ;
    uint32_t channel ;
    SSHead():seq(0),dst_key(0),src_key(0),channel(0) { } ;

    int encode(char* data,int size)
    {
        //if(size < sizeof(this) ) return -1 ;
        SSHead* head = (SSHead*)data ;
        head->seq = hton_int32(this->seq) ;
        head->dst_key= hton_int32(this->dst_key) ;
        head->src_key= hton_int32(this->src_key) ;
        head->channel= hton_int32(this->channel) ;
        return  sizeof(this) ;
    }

    int decode(const char* data,int size)
    {
        //if(size < sizeof(this) ) return -1 ;
        SSHead* head = (SSHead*)data ;

        this->seq = ntoh_int32(head->seq) ;
        this->dst_key = ntoh_int32(head->dst_key) ;
        this->src_key = ntoh_int32(head->src_key) ;
        this->channel = ntoh_int32(head->channel) ;
        return sizeof(this) ;

    }

    //int encode_size() { return sizeof(this) ; } ;

    //int decode_size(const char* data,int size) { return sizeof(this);} ;


} __attribute__((packed)) ;

struct CSHead
{
    uint32_t seq ;
    uint32_t dst_key;
    CSHead():seq(0),dst_key(0) { } ;

    int encode(char* data,int size)
    {
        //if(size < sizeof(this) ) return -1 ;
        CSHead* head = (CSHead*)data ;
        head->seq = hton_int32(this->seq) ;
        head->dst_key = hton_int32(this->dst_key) ;
        return  sizeof(this) ;
    }

    int decode(const char* data,int size)
    {
        //if(size < sizeof(this) ) return -1 ;
        CSHead* head = (CSHead*)data ;
        this->seq = ntoh_int32(head->seq) ;
        this->dst_key = ntoh_int32(head->dst_key) ;
        return sizeof(this) ;

    }

    //int encode_size() { return sizeof(this) ; } ;

    //int decode_size(const char* data,int size) { return sizeof(this);} ;


} __attribute__((packed)) ;


inline bool serialize_message(std::string& data,google::protobuf::Message& msg_body)
{
    data.resize(msg_body.ByteSize()) ;

    framework::aliased_streambuf sbuf((char*)data.c_str(), data.size() ) ;
    std::ostream output(&sbuf) ;
    try
    {
        if(!msg_body.SerializeToOstream(&output) || output.tellp() < 0 ) return false ;
    }
    catch(const std::exception& ex)
    {
        return false ;
    }

    return true ;
}

inline bool parse_message(google::protobuf::Message& msg_body,const std::string& data)
{

    framework::aliased_streambuf sbuf((char*)data.c_str(),data.size() ) ;

    std::istream input(&sbuf) ;
    try
    {
        if(!msg_body.ParseFromIstream(&input) ) return false ;
    }
    catch(const std::exception& ex)
    {
        return false ;
    }

    return true ;
}

/*
 * @brief packet template , raw packet = PacketHead + (CSHead | SSHead) + body
 * TemplatePacket = packet_type + head + body
*/
template<int PT,typename HT,typename BT>
class TemplatePacket : public framework::packet
{
public:
    enum{ packet_type = PT ,} ;
    typedef BT body_type ;
    typedef HT head_type ;
public:
    virtual int get_type() {return packet_type ; } ;

    virtual int encode(char* data,int size)
    {
        int total_head_size = sizeof(PacketHead) + sizeof(head) ;
        if(size < total_head_size ) return -1 ;

        head.encode(data +sizeof(PacketHead),sizeof(head)) ;

        framework::aliased_streambuf sbuf(data + total_head_size, size - total_head_size ) ;
        std::ostream output(&sbuf) ;
        try
        {
            if(!body.SerializeToOstream(&output) || output.tellp() < 0 ) return -1 ;
        }
        catch(const std::exception& ex)
        {
            return -1 ;
        }
        
        size = total_head_size + output.tellp() ;
        if(size >= 65535 ) return -1 ;
        PacketHead* head = (PacketHead*)data ;
        head->length = hton_int16(size) ;
        head->msg_type = hton_int16(PT) ;

        return size ;

    }

    virtual int decode(const char* data,int size)
    {
        int total_head_size = sizeof(PacketHead) + sizeof(head) ;
        if(size < total_head_size ) return -1 ;

        if(size < (int)sizeof(PacketHead) ) return -1 ;

        PacketHead* phead = (PacketHead*)data ;
        if(ntoh_int16(phead->msg_type)!= PT) return -1 ;
        size = ntoh_int16(phead->length) ;

        head.decode(data +sizeof(PacketHead),sizeof(head)) ;
        
        framework::aliased_streambuf sbuf((char*)data +total_head_size, size - total_head_size) ;

        std::istream input(&sbuf) ;
        try
        {
            if(!body.ParseFromIstream(&input) ) return -1 ;
        }
        catch(const std::exception& ex)
        {
            return -1 ;
        }
        
        return size ;

    }

    virtual int encode_size()
    {
        int need_size = (int)sizeof(PacketHead) + sizeof(head) + body.ByteSize() ;
        if( need_size >= 65535 ) return -1 ;
        return need_size ;
    }

    virtual int decode_size(const char* data,int size)
    {

        return decode_packet_size(data) ;

    }

public:
    head_type head ;
    body_type body ;
};



class NullBody
{
public:
    static bool  ParseFromIstream(std::istream* input) { return true ; } ;
    static bool  SerializeToOstream(std::ostream* output) { return true ; } ;
    static int ByteSize() { return 0 ; } ;
    static int encode(char* data,int size) { return 0 ;} ;
    static int decode(const char* data,int size) { return 0 ; } ;
    static int encode_size() {return 0 ; } ;
}  ;


