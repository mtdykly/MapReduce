package distributed;

import distributed.utils.FileUtils;
import distributed.utils.InputSplitter;
import distributed.utils.NetworkUtils;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JobTracker {
    private int port;
    private ServerSocket serverSocket;
    private Map<String, Job> jobs;                // jobId -> Job
    private Map<String, Task> tasks;              // taskId -> Task
    private Map<String, String> taskToJob;        // taskId -> jobId
    private Map<String, StatusReport> taskStatus; // taskId -> StatusReport
    private Map<String, List<String>> jobToTasks; // jobId -> List<taskId>
    private Map<String, HeartbeatMessage> taskTrackers;  // taskTrackerId -> HeartbeatMessage
    private Map<String, Integer> taskTrackerPorts;       // taskTrackerId -> port
    private Map<String, String> taskAssignments;         // taskId -> taskTrackerId
    private Set<String> completedTasks; // 已完成任务的集合，用于确保不会重复分发
    private Timer taskTrackerMonitor;   // 监控TaskTracker心跳的定时器
    private static final long HEARTBEAT_TIMEOUT = 15000;  // 15秒没有心跳则认为TaskTracker已断开
    
    public JobTracker(int port) {
        this.port = port;
        this.jobs = new ConcurrentHashMap<>();
        this.tasks = new ConcurrentHashMap<>();
        this.taskToJob = new ConcurrentHashMap<>();
        this.jobToTasks = new ConcurrentHashMap<>();
        this.taskStatus = new ConcurrentHashMap<>();
        this.taskTrackers = new ConcurrentHashMap<>();
        this.taskTrackerPorts = new ConcurrentHashMap<>();
        this.taskAssignments = new ConcurrentHashMap<>();
        this.completedTasks = new HashSet<>();
        startTaskTrackerMonitor();
    }
    
    private void startTaskTrackerMonitor() {
        taskTrackerMonitor = new Timer(true); // 守护线程定时器
        taskTrackerMonitor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long now = System.currentTimeMillis();
                List<String> deadTrackers = new ArrayList<>();
                for (Map.Entry<String, HeartbeatMessage> entry : taskTrackers.entrySet()) {
                    String trackerId = entry.getKey();
                    HeartbeatMessage lastHeartbeat = entry.getValue();
                    // 判断是否超时
                    if (now - lastHeartbeat.getTimestamp() > HEARTBEAT_TIMEOUT) {
                        System.out.println("TaskTracker " + trackerId + " appears to be dead. Last heartbeat: " + new Date(lastHeartbeat.getTimestamp()));
                        deadTrackers.add(trackerId);
                    }
                }
                // 从活跃的TaskTracker列表中移除死掉的节点
                for (String trackerId : deadTrackers) {
                    taskTrackers.remove(trackerId);
                    taskTrackerPorts.remove(trackerId);
                    System.out.println("Removed dead TaskTracker: " + trackerId);
                    // 找出分配给该TaskTracker的所有任务
                    List<String> tasksToReassign = new ArrayList<>();
                    for (Map.Entry<String, String> entry : taskAssignments.entrySet()) {
                        if (entry.getValue().equals(trackerId)) {
                            tasksToReassign.add(entry.getKey());
                        }
                    }
                    // 清理任务分配记录并重置任务状态为PENDING
                    for (String taskId : tasksToReassign) {
                        taskAssignments.remove(taskId);
                        Task task = tasks.get(taskId);
                        if (task != null && !completedTasks.contains(taskId)) {
                            task.setStatus(Task.PENDING);
                            task.setAssignedTrackerId(null);
                        }
                    }
                    // 重新分配这些任务
                    if (!tasksToReassign.isEmpty()) {
                        System.out.println("Rescheduling " + tasksToReassign.size() + " tasks from dead TaskTracker");
                        assignPendingTasks();
                    }
                }
            }
        }, HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT);
    }
    
    public void start() {
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("JobTracker started on port " + port);
            while (true) {
                try {
                    Object obj = NetworkUtils.receiveObject(serverSocket);
                    handleRequest(obj);
                } catch (Exception e) {
                    System.err.println("Error handling request: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            System.err.println("JobTrackerFix failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void handleRequest(Object obj) {
        if (obj instanceof Job) {
            handleJobSubmission((Job) obj);
        } else if (obj instanceof StatusReport) {
            handleStatusUpdate((StatusReport) obj);
        } else if (obj instanceof HeartbeatMessage) {
            handleHeartbeat((HeartbeatMessage) obj);
        } else {
            System.err.println("Unknown request type: " + obj.getClass().getName());
        }
    }
    
    // 接收客户端提交的作业请求，解析任务，拆分成Task，然后调度执行
    private void handleJobSubmission(Job job) {
        System.out.println("Received job: " + job);
        job.setStatus(Job.JobStatus.RUNNING);
        jobs.put(job.getJobId(), job);
        jobToTasks.put(job.getJobId(), new ArrayList<>());
        // 清理输出目录和中间结果目录
        String outputPath = job.getOutputPath();
        String intermediateDir = outputPath + "/intermediate";
        String outputDir = outputPath + "/output";
        // 先检查目录是否存在，如果不存在则创建
        File intermediateFile = new File(intermediateDir);
        File outputFile = new File(outputDir);
        if (!intermediateFile.exists()) {
            intermediateFile.mkdirs();
        } else {
            FileUtils.cleanDirectory(intermediateDir);
        }
        if (!outputFile.exists()) {
            outputFile.mkdirs();
        } else {
            FileUtils.cleanDirectory(outputDir);
        } 
        
        List<String> inputSplits;
        if (job.hasSpecifiedMapTasks()) {
            // 使用用户设置的Map任务数量
            inputSplits = InputSplitter.splitInputFile(job.getInputPath(), job.getNumMapTasks());
            System.out.println("Using user-specified map task count: " + job.getNumMapTasks());
        } else {
            // 根据文件大小自动确定Map任务数量
            inputSplits = InputSplitter.autoSplitInputFile(job.getInputPath());
            System.out.println("Automatically determined map task count: " + inputSplits.size());
        }

        // 根据文件大小自动确定Map任务数量
        // inputSplits = InputSplitter.autoSplitInputFile(job.getInputPath());
        // System.out.println("Automatically determined map task count: " + inputSplits.size());
        System.out.println("Job " + job.getJobId() + " will start reduce tasks when " + String.format("%.1f%%", job.getReduceStartThreshold() * 100) + " of map tasks are completed");
        // 创建并分配Map任务
        for (int i = 0; i < inputSplits.size(); i++) {
            String taskId = "map_" + job.getJobId() + "_" + i;
            Task mapTask;
            if (job.hasCombiner()) {
                // 有Combiner
                mapTask = new Task(taskId, inputSplits.get(i), intermediateDir, job.getMapperClass(), job.getReducerClass(), job.getCombinerClass(), job.getNumReducers());
                System.out.println("Created map task with combiner: " + job.getCombinerClass());
            } else {
                mapTask = new Task(taskId, inputSplits.get(i), intermediateDir, job.getMapperClass(), job.getReducerClass(), job.getNumReducers());
            }
            mapTask.setStatus(Task.PENDING);
            tasks.put(taskId, mapTask);
            taskToJob.put(taskId, job.getJobId());
            jobToTasks.get(job.getJobId()).add(taskId);            
            // 发送Map任务给TaskTracker
            assignTask(mapTask);
        }
    }
    
    // 分配任务给TaskTracker
    private void assignTask(Task task) {
        String taskId = task.getTaskId();
        if (completedTasks.contains(taskId) || taskAssignments.containsKey(taskId)) {
            System.out.println("Task " + taskId + " is already completed or assigned");
            return;
        }
        try {
            String taskTrackerId = getLeastLoadedTaskTracker(); // 找到负载最小的TaskTracker
            if (taskTrackerId != null) {
                int port = taskTrackerPorts.get(taskTrackerId);
                if (port > 0) {
                    task.setAssignedTrackerId(taskTrackerId);
                    task.setStatus(Task.RUNNING);
                    NetworkUtils.sendObject(task, NetworkUtils.LOCALHOST, port);
                    taskAssignments.put(taskId, taskTrackerId);
                    System.out.println("Sent " + task.getType() + " task: " + taskId + " to TaskTracker at port " + port);
                }
            } else {
                System.out.println("No idle TaskTracker available for task: " + taskId);
            }
        } catch (IOException e) {
            System.err.println("Failed to send task: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    // 更新各任务的执行状态
    private void handleStatusUpdate(StatusReport report) {
        String taskId = report.getTaskId();
        String status = report.getStatus();
        System.out.println("Received status report: " + report);
        taskStatus.put(taskId, report);
        // 更新Task对象的状态
        Task task = tasks.get(taskId);
        if (task != null) {
            if (status.equals("STARTED")) {
                task.setStatus(Task.RUNNING);
            } else if (status.equals("COMPLETED")) {
                task.setStatus(Task.COMPLETED);
                completedTasks.add(taskId);
            } else if (status.equals("FAILED")) {
                task.setStatus(Task.FAILED);
            }
        }
        if (status.equals("COMPLETED")) {
            // 任务完成后清理任务分配记录
            if (taskAssignments.containsKey(taskId)) {
                taskAssignments.remove(taskId);
            }
            String jobId = taskToJob.get(taskId);
            if (task.getType().equals(Task.MAP)) {
                checkAndScheduleReduceTasks(jobId);
            } else if (task.getType().equals(Task.REDUCE)) {
                checkJobCompletion(jobId);
            }
        } else if (status.equals("FAILED")) {
            // 任务分配记录中移除
            if (taskAssignments.containsKey(taskId)) {
                taskAssignments.remove(taskId);
            }
            String jobId = taskToJob.get(taskId);
            if (task != null) {
                task.setStatus(Task.PENDING); // 设置任务为PENDING状态
                task.setAssignedTrackerId(null);
                System.out.println("Task " + taskId + " failed, resetting to PENDING for retry");
                assignTask(task);// 立即尝试重新分配任务
            } else {
                // 无法获取任务信息，标记作业失败
                Job job = jobs.get(jobId);
                if (job != null) {
                    job.setStatus(Job.JobStatus.FAILED);
                    System.err.println("Job failed: " + jobId + ", due to task: " + taskId);
                }
            }
        }
    }
    
    private void handleHeartbeat(HeartbeatMessage heartbeat) {
        String taskTrackerId = heartbeat.getTaskTrackerId();
        int port = heartbeat.getPort();
        taskTrackers.put(taskTrackerId, heartbeat);
        // 更新TaskTracker的端口映射
        taskTrackerPorts.put(taskTrackerId, port);
        System.out.println("Received heartbeat from " + taskTrackerId +  ", port: " + port + ", available slots: " + (heartbeat.hasAvailableSlot() ? "YES" : "NO") + ", tasks: " + heartbeat.getRunningTasksNum() + "/" + heartbeat.getMaxTasksNum());
        // // 处理该从节点的每一个任务状态的更新
        // for (Map.Entry<String, String> entry : heartbeat.getTaskStatus().entrySet()) {
        //     String taskId = entry.getKey();
        //     String status = entry.getValue();
        //     // 只有在任务已完成时才更新状态
        //     if (status.equals("COMPLETED") && tasks.containsKey(taskId) && !completedTasks.contains(taskId)) {
        //         Task task = tasks.get(taskId);
        //         task.setStatus(Task.COMPLETED);
        //         completedTasks.add(taskId);
        //         StatusReport taskReport = new StatusReport(
        //             taskId,
        //             "COMPLETED",
        //             1.0,
        //             "Task completed according to heartbeat"
        //         );
        //         taskStatus.put(taskId, taskReport);
        //         String jobId = taskToJob.get(taskId);
        //         if (jobId != null && task.getType().equals(Task.MAP)) {
        //             checkAndScheduleReduceTasks(jobId);
        //         } else if (jobId != null && task.getType().equals(Task.REDUCE)) {
        //             checkJobCompletion(jobId);
        //         }
        //         // 任务完成后清理任务分配记录
        //         if (taskAssignments.containsKey(taskId)) {
        //             taskAssignments.remove(taskId);
        //         }
        //     }
        // }
        // if (heartbeat.hasAvailableSlot()) {
        //     assignPendingTasks(taskTrackerId);
        // }
    }
    
    // 为待处理的任务分配处理它的tasktracker
    private void assignPendingTasks() {
        List<Task> pendingTasks = getPendingTasks();
        for (Task task : pendingTasks) {
            String taskId = task.getTaskId();
            if (!taskAssignments.containsKey(taskId) && !completedTasks.contains(taskId)) {
                try {
                    String taskTrackerId = getLeastLoadedTaskTracker(); // 找到负载最小的TaskTracker
                    if (taskTrackerId != null) {
                        int port = taskTrackerPorts.get(taskTrackerId);
                        if (port > 0) {
                            task.setAssignedTrackerId(taskTrackerId);
                            task.setStatus(Task.RUNNING);
                            NetworkUtils.sendObject(task, NetworkUtils.LOCALHOST, port);
                            taskAssignments.put(taskId, taskTrackerId);
                            System.out.println("Assigned " + task.getType() + " task: " + taskId + " to TaskTracker at port " + port);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Failed to assign task " + taskId + ": " + e.getMessage());
                }
            }
        }
    }
    
    // 获取所有待处理的任务
    private List<Task> getPendingTasks() {
        List<Task> pendingTasks = new ArrayList<>();
        for (Task task : tasks.values()) {
            if (task.getType().equals(Task.MAP) && task.getStatus().equals(Task.PENDING) && !completedTasks.contains(task.getTaskId()) && !taskAssignments.containsKey(task.getTaskId())) {
                pendingTasks.add(task);
            }
        }
        if (pendingTasks.isEmpty()) {
            for (Task task : tasks.values()) {
                if (task.getType().equals(Task.REDUCE) && task.getStatus().equals(Task.PENDING) && !completedTasks.contains(task.getTaskId()) && !taskAssignments.containsKey(task.getTaskId())) {
                    pendingTasks.add(task);
                }
            }
        }
        return pendingTasks;
    }
    
    private void checkAndScheduleReduceTasks(String jobId) {
        Job job = jobs.get(jobId);
        float reduceStartThreshold = job.getReduceStartThreshold();
        // 计算已完成的Map任务数和总Map任务数
        int completedMapTasks = 0;
        int totalMapTasks = 0;
        for (String taskId : jobToTasks.get(jobId)) {
            Task t = tasks.get(taskId);
            if (t.getType().equals(Task.MAP)) {
                totalMapTasks++;
                if (completedTasks.contains(taskId) || t.getStatus().equals(Task.COMPLETED)) {
                    completedMapTasks++;
                }
            }
        }
        // 计算Map任务完成率
        float completionRate = totalMapTasks > 0 ? (float)completedMapTasks / totalMapTasks : 0;
        System.out.println("Map tasks completion rate for job " + jobId + ": " + String.format("%.1f%%", completionRate * 100) + " (" + completedMapTasks + "/" + totalMapTasks + ")");
        // 检查是否达到启动Reduce任务的阈值
        if (completionRate >= reduceStartThreshold) {
            System.out.println("Map completion rate reached threshold: " + String.format("%.1f%%", reduceStartThreshold * 100) +  ", scheduling Reduce tasks for job " + jobId);                 
            int numReducers = job.getNumReducers();
            String intermediateDir = job.getOutputPath() + "/intermediate";
            String outputDir = job.getOutputPath() + "/output";
            // 创建Reduce任务
            for (int i = 0; i < numReducers; i++) {
                String reduceTaskId = "reduce_" + jobId + "_" + i;
                if (tasks.containsKey(reduceTaskId)) {
                    continue;
                }
                Task reduceTask = new Task(reduceTaskId, i, intermediateDir, outputDir, job.getReducerClass());
                reduceTask.setStatus(Task.PENDING); // 设置初始状态为PENDING
                tasks.put(reduceTaskId, reduceTask);
                taskToJob.put(reduceTaskId, jobId);
                jobToTasks.get(jobId).add(reduceTaskId);
            }
            // 尝试分配Reduce任务
            assignPendingTasks();
        }
    }
    
    // 检查所有Reduce任务是否都已完成
    private void checkJobCompletion(String jobId) {
        boolean allReduceTasksCompleted = true;
        for (String id : jobToTasks.get(jobId)) {
            Task t = tasks.get(id);
            if (t.getType().equals(Task.REDUCE) && !completedTasks.contains(id)) {
                if (!t.getStatus().equals(Task.COMPLETED)) {
                    allReduceTasksCompleted = false;
                    break;
                }
            }
        }
        if (allReduceTasksCompleted) {
            Job job = jobs.get(jobId);
            job.setStatus(Job.JobStatus.COMPLETED);
            System.out.println("Job completed: " + job.getJobId());
            System.out.println("All tasks for job " + jobId + " have completed successfully!");
        }
    }

    private String getLeastLoadedTaskTracker() {
        String selected = null;
        double minLoad = Double.MAX_VALUE;
        for (Map.Entry<String, HeartbeatMessage> entry : taskTrackers.entrySet()) {
            HeartbeatMessage report = entry.getValue();
            if (report.hasAvailableSlot()) {
                double load = (double) report.getRunningTasksNum() / report.getMaxTasksNum();
                if (load < minLoad) {
                    minLoad = load;
                    selected = entry.getKey();
                }
            }
        }
        return selected;
    }

    public static void main(String[] args) {
        int defaultPort = NetworkUtils.JOB_TRACKER_PORT;
        int port = defaultPort;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
                NetworkUtils.setJobTrackerPort(port);
                System.out.println("Using custom port: " + port);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.err.println("Using default port: " + port);
            }
        }
        JobTracker jobTracker = new JobTracker(port);
        jobTracker.start();
    }
}
