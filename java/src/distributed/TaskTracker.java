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
    private ExecutorService executorService;      // 线程池：TaskTracker可以并发执行多个Map或Reduce任务
    private Map<String, Future<?>> runningTasks;  // taskId -> Future
    private Set<String> completedTasks;  // 已完成任务的ID集合
    private String taskTrackerId;
    private Timer heartbeatTimer;   // 定时器周期性地向JobTracker发送心跳
    private Timer taskCleanupTimer; // 定时器用于清理已完成的任务
    private String jobTrackerHost = NetworkUtils.LOCALHOST;
    private int jobTrackerPort = NetworkUtils.JOB_TRACKER_PORT;
    private static final long TASK_CLEANUP_INTERVAL = 10000; // 10秒清理一次已完成任务
    
    public TaskTracker(int port) {
        this.port = port;
        this.taskTrackerId = "TaskTracker-" + port;
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
            cleanupCompletedTasks();
            HeartbeatMessage heartbeat = new HeartbeatMessage(
                taskTrackerId, 
                runningTasks.size(), 
                5,
                port
            );
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
                task.setStatus(Task.RUNNING);
                sendStatusReport(task.getTaskId(), "STARTED", 0.0, "Task started");
                if (task.getType().equals(Task.MAP)) {
                    executeMapTask(task);
                } else if (task.getType().equals(Task.REDUCE)) {
                    executeReduceTask(task);
                } else {
                    throw new RuntimeException("Unknown task type: " + task.getType());
                }
                task.setStatus(Task.COMPLETED);
                sendStatusReport(task.getTaskId(), "COMPLETED", 1.0, "Task completed");
                completedTasks.add(task.getTaskId());
            } catch (Exception e) {
                System.err.println("Task execution failed: " + e.getMessage());
                e.printStackTrace();
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
        // 检查是否有Combiner
        Combiner<Object, Object, Object, Object> combiner = null;
        if (task.getCombinerClass() != null && !task.getCombinerClass().isEmpty()) {
            combiner = loadCombiner(task.getCombinerClass());
            System.out.println("Using Combiner: " + task.getCombinerClass());
        }
        String mapTaskId = task.getTaskId();
        String mapTaskIndex = mapTaskId.substring(mapTaskId.lastIndexOf('_') + 1);
        // 为当前Map任务创建专用输出目录
        String mapOutputDir = outputPath + "/map-" + mapTaskIndex;
        new File(mapOutputDir).mkdirs();
        // 为每个分区创建一个输出文件
        BufferedWriter[] writers = new BufferedWriter[numReducers];
        for (int i = 0; i < numReducers; i++) {
            writers[i] = new BufferedWriter(new FileWriter(mapOutputDir + "/part-" + i + ".txt"));
        }
        List<String> lines = new ArrayList<>();
        int totalLines;
        try {
            System.out.println("Reading from logical split: " + inputPath);
            lines = distributed.utils.InputSplitter.readSplit(inputPath);
            totalLines = lines.size();
            int lineCount = 0;
            // 如果有Combiner，先在内存中按key分组
            Map<String, Map<Integer, List<Object>>> keyToValues = new HashMap<>();
            if (combiner != null) {
                for (String line : lines) {
                    List<Pair<Object, Object>> results = mapper.map(line);
                    // 按key和分区收集数据
                    for (Pair<Object, Object> pair : results) {
                        String key = pair.getKey().toString();
                        int partition = PartitionUtils.getPartition(key, numReducers);
                        if (!keyToValues.containsKey(key)) {
                            keyToValues.put(key, new HashMap<>());
                        }
                        if (!keyToValues.get(key).containsKey(partition)) {
                            keyToValues.get(key).put(partition, new ArrayList<>());
                        }
                        keyToValues.get(key).get(partition).add(pair.getValue());
                    }
                    lineCount++;
                    if (lineCount % 100 == 0 || lineCount == totalLines) {
                        double progress = (double) lineCount / totalLines;
                        sendStatusReport(task.getTaskId(), "RUNNING", progress, "Processed " + lineCount + "/" + totalLines + " lines");
                    }
                }
                // 对每个分区中的键进行排序
                for (int partition = 0; partition < numReducers; partition++) {
                    Map<String, List<Object>> sortedPartitionMap = new HashMap<>();
                    for (Map.Entry<String, Map<Integer, List<Object>>> entry : keyToValues.entrySet()) {
                        String key = entry.getKey();
                        Map<Integer, List<Object>> partitionMap = entry.getValue();
                        if (partitionMap.containsKey(partition)) {
                            sortedPartitionMap.put(key, partitionMap.get(partition));
                        }
                    }
                    // 对键进行排序
                    List<String> sortedKeys = new ArrayList<>(sortedPartitionMap.keySet());
                    Collections.sort(sortedKeys);
                    for (String key : sortedKeys) {
                        List<Object> values = sortedPartitionMap.get(key);
                        Pair<Object, Object> result = combiner.combine(key, values);
                        writers[partition].write(result.getKey() + "\t" + result.getValue() + "\n");
                    }
                }
                
                System.out.println("Applied Combiner to " + keyToValues.size() + " unique keys with sorting");
            } else {
                // 没有Combiner
                Map<Integer, Map<String, List<Object>>> partitionKeyValues = new HashMap<>();
                for (int i = 0; i < numReducers; i++) {
                    partitionKeyValues.put(i, new HashMap<>());
                }
                // 收集所有结果
                for (String line : lines) {
                    List<Pair<Object, Object>> results = mapper.map(line);
                    for (Pair<Object, Object> pair : results) {
                        String key = pair.getKey().toString();
                        int partition = PartitionUtils.getPartition(key, numReducers);
                        
                        Map<String, List<Object>> partitionMap = partitionKeyValues.get(partition);
                        if (!partitionMap.containsKey(key)) {
                            partitionMap.put(key, new ArrayList<>());
                        }
                        partitionMap.get(key).add(pair.getValue());
                    }
                    lineCount++;
                    if (lineCount % 100 == 0 || lineCount == totalLines) {
                        double progress = (double) lineCount / totalLines;
                        sendStatusReport(task.getTaskId(), "RUNNING", progress, "Processed " + lineCount + "/" + totalLines + " lines");
                    }
                }
                // 对每个分区内的键进行排序并写入
                for (int partition = 0; partition < numReducers; partition++) {
                    Map<String, List<Object>> keyValues = partitionKeyValues.get(partition);
                    List<String> sortedKeys = new ArrayList<>(keyValues.keySet());
                    Collections.sort(sortedKeys);  // 键排序
                    
                    for (String key : sortedKeys) {
                        for (Object value : keyValues.get(key)) {
                            writers[partition].write(key + "\t" + value + "\n");
                        }
                    }
                }
                
                System.out.println("Map task completed with sorting for " + mapTaskId);
            }
        } finally {
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
        new File(outputDir).mkdirs();
        // 从所有Map任务中拉取对应分区的数据
        System.out.println("Reducer " + task.getTaskId() + " fetching partition " + partitionId + " from all Map tasks");
        File baseDir = new File(inputDir);
        File[] mapDirs = baseDir.listFiles((dir, name) -> name.startsWith("map-"));
        
        if (mapDirs == null || mapDirs.length == 0) {
            System.out.println("No map output directories found in " + inputDir);
            return;
        }
        
        // 从中间结果读取key-value对并按key分组
        Map<String, List<Object>> keyToValues = new HashMap<>();
        // 从每个Map任务目录中拉取对应分区的数据
        for (File mapDir : mapDirs) {
            File partitionFile = new File(mapDir, "part-" + partitionId + ".txt");
            if (!partitionFile.exists()) {
                System.out.println("No partition file in " + mapDir.getPath() + " for partition " + partitionId);
                continue;
            }
            System.out.println("Fetching data from " + partitionFile.getPath()); 
            try (BufferedReader reader = new BufferedReader(new FileReader(partitionFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        String key = parts[0];
                        // 对中间结果值进行解析（可能会有其他类型的中间结果值）
                        Object value;
                        try {
                            value = Integer.parseInt(parts[1]);
                        } catch (NumberFormatException e) {
                            value = parts[1];
                        }
                        if (!keyToValues.containsKey(key)) {
                            keyToValues.put(key, new ArrayList<>());
                        }
                        keyToValues.get(key).add(value);
                    }
                }
            }
        }
        // 按键排序
        System.out.println("Sorting keys before reduce for partition " + partitionId);
        List<String> sortedKeys = new ArrayList<>(keyToValues.keySet());
        Collections.sort(sortedKeys);  // 键排序
        
        // 对每个key调用reduce方法
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "/part-" + partitionId + ".txt"))) {
            int keyCount = 0;
            int totalKeys = sortedKeys.size();
            
            for (String key : sortedKeys) {
                List<Object> values = keyToValues.get(key);
                
                // 确保值也是有序的（可选，但有些应用场景可能需要）
                if (values.get(0) instanceof Comparable<?>) {
                    try {
                        // 使用安全的类型转换
                        @SuppressWarnings("unchecked")
                        List<Comparable<Object>> comparableValues = (List<Comparable<Object>>) (List<?>) values;
                        Collections.sort(comparableValues, Comparator.naturalOrder());
                    } catch (Exception e) {
                        // 如果排序失败，就不排序，继续处理
                        System.out.println("Warning: Could not sort values for key " + key);
                    }
                }
                
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
    
    @SuppressWarnings("unchecked")
    private Combiner<Object, Object, Object, Object> loadCombiner(String className) throws Exception {
        Class<?> combinerClass = Class.forName(className);
        return (Combiner<Object, Object, Object, Object>) combinerClass.getDeclaredConstructor().newInstance();
    }
    
    private void sendStatusReport(String taskId, String status, double progress, String message) throws IOException {
        StatusReport report = new StatusReport(taskId, status, progress, message);
        NetworkUtils.sendObject(report, jobTrackerHost, jobTrackerPort);
        System.out.println("Sent status report: " + report);
    }
    
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Error: TaskTracker port");
            System.exit(1);
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
