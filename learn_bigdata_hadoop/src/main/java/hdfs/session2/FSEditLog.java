package hdfs.session2;

import java.util.LinkedList;

/**
 * @author: reiserx
 * Date:2020/7/9
 * Des:双缓冲机制
 */
public class FSEditLog {
    public long txid = 0L;
    public DoubleBuffer editLogBuffer = new DoubleBuffer();
    private volatile Boolean isSyncRunning = false;
    private volatile Boolean isWaitSync = false;
    public volatile Long syncMaxTxid = 0L;
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();

    public static void main(String[] args) {
        FSEditLog log = new FSEditLog();
        for (int i = 0; i < 5000; i++) {
            log.logEdit("id: " + i);
        }
    }

    /**
     * 写元数据日志方法
     *
     * @param content
     */
    public void logEdit(String content) {

        synchronized (this) {
            txid++;
            EditLog log = new EditLog(txid, content);
            localTxid.set(txid);
            editLogBuffer.write(log);
        }

        /**
         * 内存1：
         * 线程1，1 元数据1
         * 线程2，2 元数据2
         * 线程3，3 元数据3
         */
        logSync();
    }

    private void logSync() {

        synchronized (this) {
            if (isSyncRunning) {

                long txid = localTxid.get();
                if (txid <= syncMaxTxid) {
                    return;
                }

                if (isWaitSync) {
                    return;
                }
                isWaitSync = true;
                while (isSyncRunning) {
                    try {
                        wait(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                isWaitSync = false;
            }

            if (editLogBuffer.currentBuffer.size() > 0) {
                syncMaxTxid = editLogBuffer.getSyncMaxTxid();
            }
            isSyncRunning = true;
        }

        editLogBuffer.flush();

        synchronized (this) {
            isSyncRunning = false;
            notifyAll();
        }

    }


}

class EditLog {
    //日志的编号，递增，唯一。
    long txid;
    //日志内容
    String content;//mkdir /data

    public EditLog(long txid, String content) {
        this.txid = txid;
        this.content = content;
    }

    @Override
    public String toString() {
        return "EditLog{" +
                "txid=" + txid +
                ", content='" + content + '\'' +
                '}';
    }
}

class DoubleBuffer {
    //内存1
    LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();
    //内存1
    LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

    public void write(EditLog log) {
        currentBuffer.add(log);
    }

    public void setReadyToSync() {
        LinkedList<EditLog> tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    public long getSyncMaxTxid() {
        return syncBuffer.getLast().txid;
    }

    public void flush() {
        for (EditLog log : syncBuffer) {
            System.out.println("存入磁盘日志信息：" + log);
        }
        //清空内存
        syncBuffer.clear();
    }
}
