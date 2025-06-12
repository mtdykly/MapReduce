package distributed.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

// 将输入文件划分为splits
public class InputSplitter {
    
    private static final long DEFAULT_BYTES_PER_MAP = 64 ; // 每个Map任务处理的数据大小(字节)
    private static final int MIN_SPLITS = 1;
    private static final int MAX_SPLITS = 10;

    // 根据文件大小自动确定切片数量并进行分割
    public static List<String> autoSplitInputFile(String inputPath) {
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Input file does not exist: " + inputPath);
            return new ArrayList<>();
        }
        long fileSize = inputFile.length();
        int calculatedSplits = (int) Math.ceil((double) fileSize / DEFAULT_BYTES_PER_MAP);
        int numSplits = Math.min(MAX_SPLITS, Math.max(MIN_SPLITS, calculatedSplits));
        System.out.println("File size: " + fileSize + " bytes, auto-determined split count: " + numSplits);
        return splitInputFile(inputPath, numSplits);
    }

    // 根据用户指定的Map任务数量
    public static List<String> splitInputFile(String inputPath, int numSplits) {
        List<String> splits = new ArrayList<>();
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Input file does not exist: " + inputPath);
            return splits;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            int totalLines = 0; // 计算文件总行数
            while (reader.readLine() != null) {
                totalLines++;
            }
            int linesPerSplit = Math.max(1, totalLines / numSplits);
            for (int i = 0; i < numSplits; i++) {
                int startLine = i * linesPerSplit;
                int endLine = (i == numSplits - 1) ? totalLines : (i + 1) * linesPerSplit;
                // 创建逻辑切片，并将其转换为字符串表示
                FileSplit split = new FileSplit(inputPath, startLine, endLine, i);
                splits.add(split.toString());
            }
        } catch (IOException e) {
            System.err.println("Error creating logical splits for input file: " + e.getMessage());
            e.printStackTrace();
        }
        return splits;
    }
    
    
    // 从逻辑切片中读取内容
    public static List<String> readSplit(String splitStr) {
        List<String> lines = new ArrayList<>();
        try {
            FileSplit split = FileSplit.fromString(splitStr);
            String filePath = split.getFilePath();
            int startLine = split.getStartLine();
            int endLine = split.getEndLine();
            try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
                // 读取切片范围内的行
                int currentLine = 0;
                String line;
                while (currentLine < startLine && reader.readLine() != null) {
                    currentLine++;
                }
                while (currentLine < endLine && (line = reader.readLine()) != null) {
                    lines.add(line);
                    currentLine++;
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading from split: " + e.getMessage());
            e.printStackTrace();
        }
        return lines;
    }
    
    // 获取指定切片的总行数
    public static int getLineCount(String splitStr) {
        FileSplit split = FileSplit.fromString(splitStr);
        return split.getEndLine() - split.getStartLine();
    }
}