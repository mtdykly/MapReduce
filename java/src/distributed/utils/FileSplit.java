package distributed.utils;

// 表示文件的一个逻辑切片
public class FileSplit {
    private String filePath;   
    private int startLine;     // 起始行号
    private int endLine;       // 结束行号
    private int splitIndex;    // 切片索引

    public FileSplit(String filePath, int startLine, int endLine, int splitIndex) {
        this.filePath = filePath;
        this.startLine = startLine;
        this.endLine = endLine;
        this.splitIndex = splitIndex;
    }

    public String getFilePath() {
        return filePath;
    }

    public int getStartLine() {
        return startLine;
    }

    public int getEndLine() {
        return endLine;
    }
    
    public int getSplitIndex() {
        return splitIndex;
    }
    
    @Override
    public String toString() {
        return filePath + "#" + startLine + "-" + endLine + "#" + splitIndex;
    }
    
    public static FileSplit fromString(String str) {
        String[] parts = str.split("#");
        String filePath = parts[0];
        String[] lineRange = parts[1].split("-");
        int startLine = Integer.parseInt(lineRange[0]);
        int endLine = Integer.parseInt(lineRange[1]);
        int splitIndex = Integer.parseInt(parts[2]);
        return new FileSplit(filePath, startLine, endLine, splitIndex);
    }
}
