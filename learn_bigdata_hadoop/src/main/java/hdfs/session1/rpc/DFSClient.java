package hdfs.session1.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author: reiserx
 * Date:2020/6/30
 * Des:
 */
public class DFSClient {

    public static void main(String[] args) throws IOException {
        ClientProtocol client = RPC.getProxy(ClientProtocol.class,
                1234L,
                new InetSocketAddress("localhost", 9989),
                new Configuration());
        System.out.println(client.deleteDir("/user/opt/data"));
        client.makeDir("/user/opt/data");

        Message message = client.put("xx.avi", "/user/opt/data");
        if (message != null && message.getCode().get() == 1)
            System.out.println(message.getMessage());
    }
}
