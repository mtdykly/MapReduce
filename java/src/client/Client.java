package client;

import distributed.*;
import distributed.utils.NetworkUtils;

import java.io.*;

public class Client {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.out.println("Usage: java client.Client <inputPath> <outputPath> <mapperClass> <reducerClass> <numReducers> [combinerClass] [numMapTasks] [jobTrackerPort]");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String mapperClass = args[2];
        String reducerClass = args[3];
        int numReducers = Integer.parseInt(args[4]);
        String combinerClass = null;
        Integer numMapTasks = null; // 用户指定的Map任务数量(可选)
        
        if (args.length > 5) {
            // 检查是否提供了Combiner类
            if (args[5] != null && !args[5].trim().isEmpty() && !args[5].equals("\"\"") && !args[5].equals("''")) {
                combinerClass = args[5];
                System.out.println("Using Combiner: " + combinerClass);
            }
        }
        
        // 检查是否提供了Map任务数量
        if (args.length > 6) {
            if (args[6] != null && !args[6].trim().isEmpty() && !args[6].equals("\"\"") && !args[6].equals("''")) {
                try {
                    numMapTasks = Integer.parseInt(args[6]);
                    System.out.println("Using specified number of Map tasks: " + numMapTasks);
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number of Map tasks, will use auto-determined value");
                }
            }
        }
        
        // 检查是否提供了JobTracker端口
        // if (args.length > 6) {
        //     int jobTrackerPort = Integer.parseInt(args[6]);
        //     NetworkUtils.setJobTrackerPort(jobTrackerPort);
        //     System.out.println("Using custom JobTracker port: " + jobTrackerPort);
        // }
        if (args.length > 7) {
            int jobTrackerPort = Integer.parseInt(args[7]);
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
        Job job;
        if (combinerClass != null && numMapTasks != null) {
            // 有Combiner且指定了Map任务数量
            job = new Job(inputPath, outputPath, mapperClass, reducerClass, combinerClass, numReducers, numMapTasks);
            System.out.println("使用Combiner和指定的Map任务数量创建作业");
        } else if (combinerClass != null) {
            // 只有Combiner
            job = new Job(inputPath, outputPath, mapperClass, reducerClass, combinerClass, numReducers);
            System.out.println("使用Combiner创建作业");
        } else if (numMapTasks != null) {
            // 只指定了Map任务数量
            job = new Job(inputPath, outputPath, mapperClass, reducerClass, numReducers, numMapTasks);
            System.out.println("使用指定的Map任务数量创建作业");
        } else {
            // 基本配置
            job = new Job(inputPath, outputPath, mapperClass, reducerClass, numReducers);
            System.out.println("使用基本配置创建作业");
        }
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
        String inputPath = "input.txt";
        String outputPath = "output";
        String mapperClass = "udf.WordCountMapper";
        String reducerClass = "udf.WordCountReducer";
        int numReducers = 3;
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.err.println("Input file does not exist: " + inputPath);
            return;
        }
        File outputDir = new File(outputPath);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        Job job = new Job(inputPath, outputPath, mapperClass, reducerClass, numReducers);
        System.out.println("Created job: " + job + " (Map tasks will be auto-determined)");
        try {
            NetworkUtils.sendObject(job, NetworkUtils.LOCALHOST, NetworkUtils.JOB_TRACKER_PORT);
            System.out.println("Job submitted to JobTracker");
            System.out.println("Job " + job.getJobId() + " is running...");
        } catch (IOException e) {
            System.err.println("Failed to submit job: " + e.getMessage());
            e.printStackTrace();
        }
    }
}