#!/bin/env python3

import argparse
import sys
import asyncio
import json
import time
import signal

class GlobalConfig:
    request = 0 
    started = 0
    stopped = 0
    connections = []

    @classmethod
    def inc_started(cls) :
        cls.started +=1

    @classmethod
    def inc_stopped(cls) :
        cls.stopped +=1

    @classmethod
    def stop_all(cls):
        print('stop all')
        for connection in  cls.connections : connection.abort()

    @classmethod
    def try_stop(cls):
        if cls.started == cls.stopped :
            loop = asyncio.get_event_loop()
            loop.stop()



class ClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.max_count = 0 
        self.request = 0
        self.success = 0
        self.total_time = 0 
        self.min_time = 9999999
        self.max_time = 0.0

    def send_request(self,data):
        self.transport.sendto(json.dumps(data).encode())
        self.request +=1 
        self.begin_request = time.time()
        #print('send "{}"'.format(data))

    def send_produce(self):
        delay = int(time.time())
        ttl = delay + 300
        retry = 30
        queue_data = 'this is queue data from request : %d' % self.request
        data = {'action':1,'queue':'test1','delay':delay,'ttl':ttl,'retry':retry,'data':queue_data}
        self.send_request(data)

    def send_consume(self):
        data = {'action':2,'queue':'test1'}
        self.send_request(data)

    def send_confirm(self,queue,msg_id):
        data = { 'action':3,'msg_id':msg_id,'queue':queue }
        self.send_request(data)


    def on_start(self):
        if self.request >= GlobalConfig.request : self.transport.close()
        else : self.send_produce()

    def on_result(self,response):
        if 'msg_id' in response and response['action'] in [1,2] :
            queue = response['queue']
            msg_id = int(response['msg_id'])
            self.send_confirm(queue,msg_id) 
        else :
            self.on_start()


    def connection_made(self, transport):
        GlobalConfig.started +=1 
        self.transport = transport
        self.begin_time = time.time()
        self.on_start()

    def datagram_received(self, data, addr):
        #print('received "{}"'.format(data))
        try:
            request_time = time.time() - self.begin_request
            if request_time > 0.0 :
                self.total_time += request_time
                if request_time > self.max_time : self.max_time = request_time
                if request_time < self.min_time : self.min_time = request_time

            response = json.loads(data.decode())
            if response['code'] == 0 : self.success +=1 
            self.on_result(response)
        except Exception as ex:
            print(ex)


    def error_received(self, exc):
        print('Error received:', exc)

    def connection_lost(self, exc):
        consume_time = time.time() - self.begin_time
        avg_time = self.total_time/self.request if self.request >0 else 0.0
        print('counter:%d success:%d consume:%f avg:%f min:%f max:%f' % 
            (self.request,self.success,consume_time,avg_time,self.min_time,self.max_time) )
        GlobalConfig.stopped +=1
        GlobalConfig.try_stop()




def parse_args():
    parser = argparse.ArgumentParser(description="bench tool")
    parser.add_argument( '--host', dest='host', default='127.0.0.1', help='Host name')
    parser.add_argument( '--port', dest='port', default=1111, type=int, help='Port number')
    parser.add_argument( '-c', dest='concurrent', default=8, type=int, help='concurrent count') 
    parser.add_argument( '-n', dest='request', default=1000, type=int, help='request count')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    if ':' in args.host:
        args.host, port = args.host.split(':', 1)
        args.port = int(port)

    GlobalConfig.request = args.request

    loop = asyncio.get_event_loop()
    if signal is not None:
        loop.add_signal_handler(signal.SIGINT, GlobalConfig.stop_all )

    for x in range(0,args.concurrent):
        t = asyncio.Task(loop.create_datagram_endpoint(ClientProtocol,remote_addr=(args.host, args.port)))
        loop.run_until_complete(t)
        transport,protocol = t.result()
        protocol.max_count = args.request
        GlobalConfig.connections.append(transport)

    try:
        loop.run_forever()
        
    finally:
        loop.close()

