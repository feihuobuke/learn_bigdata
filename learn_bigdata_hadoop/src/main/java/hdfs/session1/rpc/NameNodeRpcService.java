package hdfs.session1.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.IOException;

/**
 * @author: reiserx
 * Date:2020/6/30
 * Des:rpc 服务器
 */
public class NameNodeRpcService implements ClientProtocol {
    @Override
    public void makeDir(String path) {
        System.out.println("新建目录" + path);
    }

    @Override
    public String deleteDir(String path) {
        System.out.println("删除目录" + path);
        return "删除目录" + path + "成功";
    }

    @Override
    public Message put(String file, String filePath) {
        return new Message(1, filePath + "put 成功");
    }

    public static void main(String[] args) throws IOException {
        Server server = new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(9989)
                .setProtocol(ClientProtocol.class)
                .setInstance(new NameNodeRpcService())
                .build();

        System.out.println("服务器启动");
        server.start();
    }
}
