package org.apache.spark

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/06/25 13:49
 * Description：
 */
object RpcClientTest {
    
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
        
        val sparkSession = SparkSession.builder().config(conf).master("local[*]").appName("test rpc").getOrCreate()
        val sparkContext: SparkContext = sparkSession.sparkContext
        val sparkEnv: SparkEnv = sparkContext.env
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： RpcEnv
         */
        val rpcEnv: RpcEnv = RpcEnv
            .create(HelloRpcSettings.getName(), HelloRpcSettings.getHostname(), HelloRpcSettings.getPort(), conf, sparkEnv.securityManager, false)
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： endPointRef
         */
        val endPointRef: RpcEndpointRef = rpcEnv
            .setupEndpointRef(RpcAddress(HelloRpcSettings.getHostname(), HelloRpcSettings.getPort()), HelloRpcSettings.getName())
        
        import scala.concurrent.ExecutionContext.Implicits.global
        
        // TODO_MA 注释：
        endPointRef.send(SayHi("test send"))
        
        // TODO_MA 注释：
        val future: Future[String] = endPointRef.ask[String](SayHi("neo"))
        future.onComplete { case scala.util.Success(value) => println(s"Got the result = $value")
        case scala.util.Failure(e) => println(s"Got error: $e")
        }
        Await.result(future, Duration.apply("30s"))

        // TODO_MA 注释：
        val res = endPointRef.askSync[String](SayBye("test askSync"))
        println(res)
        
        sparkSession.stop()
    }
}
