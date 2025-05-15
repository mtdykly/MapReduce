package client;

import distributed.*;
import distributed.utils.NetworkUtils;

import java.io.*;

public class Client {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage: java client.Client <inputPath> <outputPath> <mapperClass> <reducerClass> <numReducers> [jobTrackerPort]");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String mapperClass = args[2];
        String reducerClass = args[3];
        int numReducers = Integer.parseInt(args[4]);
        if (args.length > 5) {
            int jobTrackerPort = Integer.parseInt(args[5]);
            NetworkUtils.setJobTrackerPort(jobTrackerPort);
            System.out.println("Using custom JobTracker port: " + jobTrackerPort);
        }
        // 检查输入文件是否存在
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("输入文件不存在，路径：" + inputPath);
            System.exit(1);
        }
        // 创建输出目录
        File outputDir = new File(outputPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        // 创建并提交作业
        Job job = new Job(inputPath, outputPath, mapperClass, reducerClass, numReducers);
        System.out.println("Created job: " + job);
        try {
            // 将作业发送给JobTracker
            NetworkUtils.sendObject(job, NetworkUtils.LOCALHOST, NetworkUtils.JOB_TRACKER_PORT);
            System.out.println("Job submitted to JobTracker");
            System.out.println("Job " + job.getJobId() + " is running");
        } catch (IOException e) {
            System.err.println("Failed to submit job: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // WordCount
    public static void startWordCount() {
        // 为测试WordCount准备的方法
        String inputPath = "input.txt";
        String outputPath = "output";
        String mapperClass = "udf.WordCountMapper";
        String reducerClass = "udf.WordCountReducer";
        int numReducers = 3;
        // 检查输入文件是否存在
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Input file does not exist: " + inputPath);
            return;
        }
        // 创建输出目录
        File outputDir = new File(outputPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        // 创建并提交作业
        Job job = new Job(inputPath, outputPath, mapperClass, reducerClass, numReducers);
        System.out.println("Created job: " + job);
        try {
            // 将作业发送给JobTracker
            NetworkUtils.sendObject(job, NetworkUtils.LOCALHOST, NetworkUtils.JOB_TRACKER_PORT);
            System.out.println("Job submitted to JobTracker");
            System.out.println("Job " + job.getJobId() + " is running...");
        } catch (IOException e) {
            System.err.println("Failed to submit job: " + e.getMessage());
            e.printStackTrace();
        }
    }
}