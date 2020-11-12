package org.apache.spark

/**
 * Author：马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * DateTime：2020/06/25 13:40
 * Description：
 */
object HelloRpcSettings {
    val rpcName = "hello-rpc-service"
    val port = 9527
    val hostname = "localhost"
    
    def getName() = {
        rpcName
    }
    
    def getPort(): Int = {
        port
    }
    
    def getHostname(): String = {
        hostname
    }
}
