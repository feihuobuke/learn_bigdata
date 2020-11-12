package hdfs.session1.rpc;

public interface ClientProtocol {
    long versionID = 1234L;

    void makeDir(String path);

    String deleteDir(String path);

    Message put(String file, String filePath);


}
