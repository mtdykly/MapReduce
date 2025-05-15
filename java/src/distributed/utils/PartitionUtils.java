package distributed.utils;

// 分区工具类
public class PartitionUtils {
    // 计算给定key的分区ID
    public static int getPartition(String key, int numReducers) {
        return Math.abs(key.hashCode() % numReducers);
    }
}