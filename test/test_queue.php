<?php

class QueueClient
{
    private $sock = null ;
    private $tcp_flag = false ;
    private $timeout = null ;
    private $host_list = null ;
    private $queue_name = '' ;
    private $master = null ;

    function __construct( $host_list,$queue_name='',$tcp_flag=false,$timeout=2)
    {
        $this->host_list = $host_list ;
        $this->queue_name = $queue_name ;
        $this->tcp_flag =  $tcp_flag ;
        $this->timeout =  array('sec' => $timeout, 'usec' => 0) ;

    }

    function close()
    {
        if($this->sock)  
        {
            socket_close($this->sock) ;
            $this->sock = null ; 
        }
     
    }
   
       
    /*
     * @brief  send request and recv response 
     * @array
     */
    private function do_request($request)
    {
        $seq = rand() ;
        $request['seq']=$seq ;
        $send_data = json_encode($request) ;
        foreach($this->host_list as $host)
        {
            if(!empty($this->master)) $host = $this->master ;
            $this->close() ;  
            if($this->tcp_flag) $this->sock = socket_create(AF_INET, SOCK_STREAM,SOL_TCP);
            else $this->sock = socket_create(AF_INET, SOCK_DGRAM,SOL_UDP);
            socket_set_option($this->sock, SOL_SOCKET, SO_RCVTIMEO, $this->timeout);
            socket_set_option($this->sock, SOL_SOCKET, SO_SNDTIMEO, $this->timeout);
            if(!socket_connect($this->sock,$host['host'], $host['port']) ) continue ;

            $ret = socket_send($this->sock, $send_data, strlen($send_data), 0);
            if($ret != strlen($send_data) )  continue ;
            $ret = socket_recv($this->sock, $buf, 10240 , 0);
            $this->close() ;
            if(!$ret) continue ;
            $result = json_decode($buf,true) ;
            if ( is_array($result) && $result['seq'] == $seq )
            {
                if( $result['code'] == -2 ) 
                {
                    $this->master = array('host'=>$result['master_host'],'port'=>$result['master_port']) ;
                    continue ;
                }
                else
                {
                    unset($result['seq']) ;
                    return $result ;
                }
            }
            
        }

        return null ;

    }

    /*
     * @brief  set current queue name
     * 
     */
    function set_queue_name($queue_name)
    {
        $this->queue_name = $queue_name ;
    }


    /*
     * @brief push message into queue
     * @return array , array['code'] , array['msg_id'] 
     */
    function produce($data,$delay,$retry,$ttl)
    {
        if(is_array($data) ) $data = json_encode($data) ;
        $request = array( 
            "action"=>1 , 
            "queue"=>$this->queue_name , 
            "data" => $data ,
            "delay"=> $delay, 
            "ttl"=> $ttl,
            "retry"=> $retry,
        );

        return $this->do_request($request) ;
    }

    /*
     * @brief pop message from queue
     * @return array , array['code'] , array['msg_id']  , array['data']
     */
    function consume()
    {
        $now = time() ;
        $request = array( "action"=>2 , "queue"=>$this->queue_name , );
        return $this->do_request($request) ;
        
    }

    /*
     * @brief confirm message 
     * @return array , array['code'] 
     */
    function confirm($msg_id)
    {
        $now = time() ;
        $request = array( "action"=>3 , "queue"=>$this->queue_name ,"msg_id"=>$msg_id,);
        return $this->do_request($request) ;
    }

    /*
     * @brief monitor queue
     * @return array , array['code']  ,array['size'] , array['max_size']
     */
    function monitor()
    {
        $now = time() ;
        $request = array( "action"=>104 , "queue"=>$this->queue_name );
        return $this->do_request($request) ;
    }

    function list_queue()
    {
        $now = time() ;
        $request = array( "action"=>7);
        return $this->do_request($request) ;
    }


}

function consume_confirm($client)
{
    $result = $client->consume() ;
    if(is_array($result) && isset($result["msg_id"]) ) 
    {
        $client->confirm($result["msg_id"]) ;
    }

    return $result;
}


function bench($host_list,$count)
{
    $min_time = 1000000.0 ;
    $max_time = 0.0000001;
    $fail = 0 ;
    $total_time = 0.0000001;

    $client = new QueueClient($host_list,"test_queue") ;

    for($i=0 ; $i < $count ; ++$i)
    {
        $begin_time = microtime() ;
        $now = time() ;
        $result = $client->produce(array("order_id"=>$i),$now,60,$now+3600) ;
        //$result = consume_confirm($client) ;
        //var_dump($result) ;
        if(empty($result) || empty($result["msg_id"]) )
        {
            ++$fail ;
        }
       
        $consume_time = microtime() - $begin_time ;
        if($consume_time >0.0000001)
        {
            if($min_time > $consume_time) $min_time = $consume_time ;
            if($max_time < $consume_time) $max_time = $consume_time ;
            $total_time += $consume_time ;
        }
    }

    printf("total:%d fail:%d min:%f max:%f avg:%f\n",$count,$fail,$min_time,$max_time,$total_time/$count) ;
}

function bench_process($process,$count,$host_list)
{

    for($i = 0 ; $i < $process ; ++$i)
    {

        $pid = pcntl_fork() ;
        if($pid ==0)
        {
            sleep(1) ;
            bench($host_list,$count) ;
            exit(0) ;
        }
    }

    exit(0) ;
}


$host_list = array(
    array('host'=>'127.0.0.1','port'=>1111), 
) ;

//bench_process(4,10000,$host_list) ;
//exit ;
//bench($host_list,250) ;

$begin_time = microtime() ;
$station_id = "test" ;
$queue_name = "task#${station_id}:order:event" ;
$queue_name = 'test_queue' ;

$client = new QueueClient($host_list,"test",false) ;
var_dump($client->produce(array("title"=>$queue_name),time(),60,time()+3600  ) );
var_dump($client->list_queue()) ;
//$msg = $client->consume();
//$msg_id =  $msg['msg_id'] ;
//if ($msg_id >0) var_dump($client->confirm($msg_id) ) ;

echo "consume_time :" . (microtime() - $begin_time) . "\n" ;   

?>
