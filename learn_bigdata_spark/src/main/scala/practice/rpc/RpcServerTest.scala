package org.apache.spark

import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.sql.SparkSession

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/06/25 13:46
 * Description：
 */
object RpcServerTest {
    
    def main(args: Array[String]): Unit = {
        
        val conf: SparkConf = new SparkConf()
        
        val sparkSession = SparkSession.builder().config(conf).master("local[*]").appName("test rpc").getOrCreate()
        val sparkContext: SparkContext = sparkSession.sparkContext
        val sparkEnv: SparkEnv = sparkContext.env
        
        /**
         * TODO_MA 马中华 https://blog.csdn.net/zhongqi2513
         *   注释： 系统
         */
        val rpcEnv = RpcEnv
            .create(HelloRpcSettings.getName(), HelloRpcSettings.getHostname(), HelloRpcSettings.getHostname(), HelloRpcSettings.getPort(), conf,
                sparkEnv.securityManager, 1, false)
        
        // TODO_MA 注释：创建 和启动 endpoint
        val helloEndpoint: RpcEndpoint = new HelloEndPoint(rpcEnv)
        rpcEnv.setupEndpoint(HelloRpcSettings.getName(), helloEndpoint)
        
        rpcEnv.awaitTermination()
        
    }
}
