package udf;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import distributed.Mapper;
import distributed.Pair;

public class WordCountMapper implements Mapper<String, Integer> {

    @Override
    public List<Pair<String, Integer>> map(String line) {
        String[] words = line.split(" ");
        return Arrays.stream(words).map(word -> new Pair<>(word, 1)).collect(Collectors.toList());
    }
}
