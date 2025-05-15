package distributed.utils;

import java.io.*;

// 文件操作工具类
public class FileUtils {
    
    public static void createDir(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    //创建中间结果目录
    public static void createIntermediateDir(String basePath) {
        createDir(basePath + "/intermediate");
    }
}