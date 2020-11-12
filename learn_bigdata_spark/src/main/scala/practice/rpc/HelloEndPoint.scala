package org.apache.spark

import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/06/25 13:41
 * Description：
 */
class HelloEndPoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {
    
    override def onStart(): Unit = {
        println(rpcEnv.address)
        println("start hello endpoint")
    }
    
    override def receive: PartialFunction[Any, Unit] = {
        case SayHi(msg) => println(s"receive $msg")
    }
    
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
        case SayHi(msg) => {
            println(s"receive $msg")
            context.reply(s"hi, $msg")
        }
        case SayBye(msg) => {
            println(s"receive $msg")
            context.reply(s"bye, $msg")
        }
    }
    
    override def onStop(): Unit = {
        println("stop hello endpoint")
    }
}

case class SayHi(msg: String)

case class SayBye(msg: String)