package distributed;

import distributed.utils.NetworkUtils;
import distributed.utils.PartitionUtils;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class TaskTracker {
    private int port;
    private ServerSocket serverSocket;
    private ExecutorService executorService; // 线程池：TaskTracker可以并发执行多个Map或Reduce任务
    private Map<String, Future<?>> runningTasks;  // taskId -> Future
    private Set<String> completedTasks;  // 已完成任务的ID集合
    private String taskTrackerId;
    private Timer heartbeatTimer;   // 定时器周期性地向JobTracker发送心跳
    private Timer taskCleanupTimer; // 定时器用于清理已完成的任务
    private String jobTrackerHost = NetworkUtils.LOCALHOST;
    private int jobTrackerPort = NetworkUtils.JOB_TRACKER_PORT;
    private boolean busy = false;
    private static final long TASK_CLEANUP_INTERVAL = 10000; // 10秒清理一次已完成任务
    
    public TaskTracker(int port) {
        this.port = port;
        this.taskTrackerId = "TaskTracker-" + port;  // 使用端口号作为唯一标识符
        this.executorService = Executors.newFixedThreadPool(5);  // 最多同时执行5个任务
        this.runningTasks = new ConcurrentHashMap<>();
        this.completedTasks = Collections.synchronizedSet(new HashSet<>());
    }
    
    // 设置JobTracker的主机和端口
    public void setJobTrackerAddress(String host, int port) {
        this.jobTrackerHost = host;
        this.jobTrackerPort = port;
        System.out.println("Set JobTracker address to " + host + ":" + port);
    }
    
    public void start() {
        // 启动心跳线程
        startHeartbeat();
        // 启动任务清理线程
        startTaskCleanup();
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("TaskTracker started on port " + port);
            while (true) {
                try {
                    Object obj = NetworkUtils.receiveObject(serverSocket);
                    if (obj instanceof Task) {
                        Task task = (Task) obj;
                        System.out.println("Received task: " + task);
                        busy = true;  // 标记TaskTracker为忙碌状态
                        executeTask(task);
                    } else {
                        System.err.println("Unknown request type: " + obj.getClass().getName());
                    }
                } catch (Exception e) {
                    System.err.println("Error handling task: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("TaskTracker failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭心跳线程
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel();
            }
            // 关闭任务清理线程
            if (taskCleanupTimer != null) {
                taskCleanupTimer.cancel();
            }
        }
    }
    
    private void startHeartbeat() {
        heartbeatTimer = new Timer(true); // 守护线程
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                sendHeartbeat();
            }
        }, 0, 10000); // 每10秒发送一次
        System.out.println("Heartbeat thread started");
    }
    
    private void startTaskCleanup() {
        taskCleanupTimer = new Timer(true); // 守护线程
        taskCleanupTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                cleanupCompletedTasks();
            }
        }, TASK_CLEANUP_INTERVAL, TASK_CLEANUP_INTERVAL);
        System.out.println("Task cleanup thread started");
    }
    
    // 清理已完成的任务
    private void cleanupCompletedTasks() {
        List<String> tasksToRemove = new ArrayList<>();
        
        for (Map.Entry<String, Future<?>> entry : runningTasks.entrySet()) {
            String taskId = entry.getKey();
            Future<?> future = entry.getValue();
            
            if (future.isDone() || future.isCancelled() || completedTasks.contains(taskId)) {
                tasksToRemove.add(taskId);
            }
        }
        
        for (String taskId : tasksToRemove) {
            runningTasks.remove(taskId);
            System.out.println("Cleaned up completed task: " + taskId);
        }
        
        if (!tasksToRemove.isEmpty()) {
            System.out.println("Cleaned up " + tasksToRemove.size() + " completed tasks. Current running tasks: " + runningTasks.size());
        }
    }
    
    private void sendHeartbeat() {
        try {
            // 在发送心跳前清理已完成的任务
            cleanupCompletedTasks();
            
            HeartbeatMessage heartbeat = new HeartbeatMessage(
                taskTrackerId, 
                runningTasks.size(), 
                5,  // 最大任务数为5
                port  // 发送当前TaskTracker的端口号
            );
            // 添加当前正在执行的任务状态
            for (Map.Entry<String, Future<?>> entry : runningTasks.entrySet()) {
                String taskId = entry.getKey();
                Future<?> future = entry.getValue();
                String status = future.isDone() ? "COMPLETED" : "RUNNING";
                heartbeat.addTaskStatus(taskId, status);
            }
            NetworkUtils.sendObject(heartbeat, jobTrackerHost, jobTrackerPort);
            System.out.println("Sent heartbeat: " + heartbeat + ", running tasks: " + runningTasks.size() + "/5");
        } catch (Exception e) {
            System.err.println("Failed to send heartbeat: " + e.getMessage());
        }
    }

    private void executeTask(final Task task) {
        Future<?> future = executorService.submit(() -> {
            try {
                // 更新任务状态为运行中
                task.setStatus(Task.RUNNING);
                sendStatusReport(task.getTaskId(), "STARTED", 0.0, "Task started");
                if (task.getType().equals(Task.MAP)) {
                    executeMapTask(task);
                } else if (task.getType().equals(Task.REDUCE)) {
                    executeReduceTask(task);
                } else {
                    throw new RuntimeException("Unknown task type: " + task.getType());
                }
                // 更新任务状态为已完成
                task.setStatus(Task.COMPLETED);
                sendStatusReport(task.getTaskId(), "COMPLETED", 1.0, "Task completed");
                
                // 标记任务已完成，等待清理
                completedTasks.add(task.getTaskId());
            } catch (Exception e) {
                System.err.println("Task execution failed: " + e.getMessage());
                e.printStackTrace();
                // 更新任务状态为失败
                task.setStatus(Task.FAILED);
                try {
                    sendStatusReport(task.getTaskId(), "FAILED", 0.0, e.getMessage());
                } catch (Exception ex) {
                    System.err.println("Failed to send status report: " + ex.getMessage());
                    ex.printStackTrace();
                }
            }
        });
        
        runningTasks.put(task.getTaskId(), future);
    }
    
    private void executeMapTask(Task task) throws Exception {
        String inputPath = task.getInputPath();
        String outputPath = task.getOutputPath();
        int numReducers = task.getNumReducers();
        // 动态加载Mapper类
        Mapper<Object, Object> mapper = loadMapper(task.getMapperClass());
        // 创建输出目录
        new File(outputPath).mkdirs();
        // 为每个Reducer创建一个中间输出文件
        BufferedWriter[] writers = new BufferedWriter[numReducers];
        for (int i = 0; i < numReducers; i++) {
            writers[i] = new BufferedWriter(new FileWriter(outputPath + "/part-" + i + ".txt"));
        }
        // 读取输入文件并执行map
        try (BufferedReader reader = new BufferedReader(new FileReader(inputPath))) {
            String line;
            int lineCount = 0;
            int totalLines = countLines(inputPath);
            
            while ((line = reader.readLine()) != null) {
                List<Pair<Object, Object>> results = mapper.map(line);
                // 将结果写入对应的中间文件（按key的hash分区）
                for (Pair<Object, Object> pair : results) {
                    String key = pair.getKey().toString();
                    int partition = PartitionUtils.getPartition(key, numReducers);
                    writers[partition].write(key + "\t" + pair.getValue() + "\n");
                }
                lineCount++;
                if (lineCount % 100 == 0 || lineCount == totalLines) {
                    double progress = (double) lineCount / totalLines;
                    sendStatusReport(task.getTaskId(), "RUNNING", progress, 
                                    "Processed " + lineCount + "/" + totalLines + " lines");
                }
            }
        } finally {
            // 关闭所有writers
            for (BufferedWriter writer : writers) {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }
    
    private void executeReduceTask(Task task) throws Exception {
        int partitionId = task.getPartitionId();
        String inputDir = task.getInputPath();
        String outputDir = task.getOutputPath();
        // 动态加载Reducer类
        Reducer<Object, Object, Object, Object> reducer = loadReducer(task.getReducerClass());
        // 创建输出目录
        new File(outputDir).mkdirs();
        // 读取对应的中间结果文件
        String inputPath = inputDir + "/part-" + partitionId + ".txt";
        File inputFile = new File(inputPath);
        if (!inputFile.exists()) {
            System.out.println("No intermediate file for partition " + partitionId);
            return;
        }
        // 从中间结果读取key-value对并按key分组
        Map<String, List<Object>> keyToValues = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String key = parts[0];
                    // 对中间结果值进行解析，尝试转换为Integer
                    Object value;
                    try {
                        value = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException e) {
                        value = parts[1]; // 如果转换失败，保持字符串形式
                    }
                    
                    if (!keyToValues.containsKey(key)) {
                        keyToValues.put(key, new ArrayList<>());
                    }
                    keyToValues.get(key).add(value);
                }
            }
        }
        // 对每个key调用reduce方法
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "/part-" + partitionId + ".txt"))) {
            int keyCount = 0;
            int totalKeys = keyToValues.size();
            for (Map.Entry<String, List<Object>> entry : keyToValues.entrySet()) {
                String key = entry.getKey();
                List<Object> values = entry.getValue();
                Pair<Object, Object> result = reducer.reduce(key, values);
                writer.write(result.getKey() + "\t" + result.getValue() + "\n");
                keyCount++;
                if (keyCount % 100 == 0 || keyCount == totalKeys) {
                    double progress = (double) keyCount / totalKeys;
                    sendStatusReport(task.getTaskId(), "RUNNING", progress, "Reduced " + keyCount + "/" + totalKeys + " keys");
                }
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private Mapper<Object, Object> loadMapper(String className) throws Exception {
        Class<?> mapperClass = Class.forName(className);
        return (Mapper<Object, Object>) mapperClass.getDeclaredConstructor().newInstance();
    }
    
    @SuppressWarnings("unchecked")
    private Reducer<Object, Object, Object, Object> loadReducer(String className) throws Exception {
        Class<?> reducerClass = Class.forName(className);
        return (Reducer<Object, Object, Object, Object>) reducerClass.getDeclaredConstructor().newInstance();
    }
    
    private int countLines(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int count = 0;
            while (reader.readLine() != null) {
                count++;
            }
            return count;
        }
    }
    
    private void sendStatusReport(String taskId, String status, double progress, String message) throws IOException {
        StatusReport report = new StatusReport(taskId, status, progress, message);
        NetworkUtils.sendObject(report, jobTrackerHost, jobTrackerPort);
        System.out.println("Sent status report: " + report);
    }
    
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Error: TaskTracker port");
            System.exit(1);  // 非正常退出，表示参数错误
        }
        int port;
        try {
            port = Integer.parseInt(args[0]);
            System.out.println("Using custom task tracker port: " + port);
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: " + args[0]);
            System.exit(1);
            return;
        }
        int jobTrackerPort = NetworkUtils.JOB_TRACKER_PORT;
        TaskTracker taskTracker = new TaskTracker(port);
        taskTracker.setJobTrackerAddress(NetworkUtils.LOCALHOST, jobTrackerPort);
        taskTracker.start();
    }
}
