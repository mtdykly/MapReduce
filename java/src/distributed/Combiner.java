package distributed;

import java.util.List;

public interface Combiner<K, V, OK, OV> {
    // 在Map端对具有相同key的中间结果进行合并
    Pair<OK, OV> combine(K key, List<V> values);
}
