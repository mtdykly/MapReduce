package udf;

import java.util.List;

import distributed.Pair;
import distributed.Reducer;

public class WordCountReducer implements Reducer<String, Integer, String, Integer> {

    @Override
    public Pair<String, Integer> reduce(String key, List<Integer> values) {
        return new Pair<String,Integer>(key, values.stream().mapToInt(e -> e).sum());
    }
}
