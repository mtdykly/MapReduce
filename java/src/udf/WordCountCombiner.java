package udf;

import distributed.Combiner;
import distributed.Pair;

import java.util.List;

// WordCountCombiner: 在Map阶段对相同key的单词计数进行局部合并
public class WordCountCombiner implements Combiner<String, Object, String, Integer> {
    
    @Override
    public Pair<String, Integer> combine(String key, List<Object> values) {
        int sum = 0;
        for (Object value : values) {
            if (value instanceof Integer) {
                sum += (Integer) value;
            } else if (value instanceof String) {
                try {
                    sum += Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    // 忽略无法解析为数字的值
                }
            }
        }

        return new Pair<>(key, sum);
    }
}
