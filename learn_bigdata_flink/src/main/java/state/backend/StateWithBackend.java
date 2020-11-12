package state.backend;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.runtime.state.memory.MemoryStateBackend.DEFAULT_MAX_STATE_SIZE;

/**
 * @author: reiserx
 * Date:2020/11/3
 * Des:Backend 测试
 * 1。MemoryStateBackend
 * 2.FsStateBackend
 * 3.RocksDBStateBackend
 */
public class StateWithBackend {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new MemoryStateBackend(DEFAULT_MAX_STATE_SIZE,false));
        env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints",false));
//        env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:40010/flink/checkpoints",false));


    }
}
