package distributed.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

// 输入文件拆分工具类
public class InputSplitter {

    public static List<String> splitInputFile(String inputPath, int numSplits) {
        List<String> splits = new ArrayList<>();
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Input file does not exist: " + inputPath);
            return splits;
        }
        // 简单将文件按行分割，而不是按大小分割
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            int linesPerSplit = Math.max(1, lines.size() / numSplits);
            for (int i = 0; i < numSplits; i++) {
                String splitPath = inputPath + ".split" + i;
                int startLine = i * linesPerSplit;
                int endLine = (i == numSplits - 1) ? lines.size() : (i + 1) * linesPerSplit;
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(splitPath))) {
                    for (int j = startLine; j < endLine; j++) {
                        writer.write(lines.get(j));
                        writer.newLine();
                    }
                }
                splits.add(splitPath);
            }
        } catch (IOException e) {
            System.err.println("Error splitting input file: " + e.getMessage());
            e.printStackTrace();
        }
        return splits;
    }
}