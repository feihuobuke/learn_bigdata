package basic.streaming.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author: reiserx
 * Date:2020/10/24
 * Des:自定义支持并行度的数据源，每秒产生一条数据
 */
public class MyParalleSource implements ParallelSourceFunction<Long> {
    private long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(number);
            System.out.println("Thread[" + Thread.currentThread().getName() + ":" + Thread.currentThread().getId() + "] 产生数据 " + number);
            number++;
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
