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
    
    // 清理目录，删除目录下的所有文件和子目录
    public static boolean cleanDirectory(String dirPath) {
        File dir = new File(dirPath);
        if (!dir.exists()) {
            return true;
        }
        boolean success = true;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    success = success && cleanDirectory(file.getAbsolutePath());
                }
                if (!file.delete()) {
                    System.err.println("Failed to delete: " + file.getAbsolutePath());
                    success = false;
                }
            }
        }
        return success;
    }
}