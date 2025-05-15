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
        taskTrackerMonitor = new Timer(true); // 创建一个守护线程定时器
        taskTrackerMonitor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                checkTaskTrackers();
            }
        }, HEARTBEAT_TIMEOUT, HEARTBEAT_TIMEOUT);
        // System.out.println("TaskTracker monitor started");
    }
    
    private void checkTaskTrackers() {
        long now = System.currentTimeMillis();
        List<String> deadTrackers = new ArrayList<>();
        for (Map.Entry<String, HeartbeatMessage> entry : taskTrackers.entrySet()) {
            String trackerId = entry.getKey();
            HeartbeatMessage lastHeartbeat = entry.getValue();
            if (now - lastHeartbeat.getTimestamp() > HEARTBEAT_TIMEOUT) {
                System.out.println("TaskTracker " + trackerId + " appears to be dead. Last heartbeat: " + new Date(lastHeartbeat.getTimestamp()));
                deadTrackers.add(trackerId);
            }
        }
        // 从活跃TaskTracker列表中移除死掉的节点
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
            // 后续可以添加任务重新分配的逻辑
        }
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
        
        // 将输入文件分割成多个splits
        List<String> inputSplits = InputSplitter.splitInputFile(job.getInputPath(), 3); // 默认3个splits
        // 创建并分配Map任务
        for (int i = 0; i < inputSplits.size(); i++) {
            String taskId = "map_" + job.getJobId() + "_" + i;
            String intermediateDir = job.getOutputPath() + "/intermediate";
            FileUtils.createIntermediateDir(job.getOutputPath());
            Task mapTask = new Task(taskId, inputSplits.get(i), intermediateDir,
                                   job.getMapperClass(), job.getReducerClass(), job.getNumReducers());
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
            String taskTrackerId = getIdleTaskTracker(); // 先找到一个空闲的TaskTracker
            if (taskTrackerId != null) {
                int port = getTaskTrackerPort(taskTrackerId);
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
            // System.out.println("Updated task status: " + task);
        }
        if (status.equals("COMPLETED")) {
            // 任务完成后清理任务分配记录
            if (taskAssignments.containsKey(taskId)) {
                // System.out.println("Task " + taskId + " completed, removing assignment record");
                taskAssignments.remove(taskId);
            }
            String jobId = taskToJob.get(taskId);
            if (task.getType().equals(Task.MAP)) {
                checkAndScheduleReduceTasks(jobId);
            } else if (task.getType().equals(Task.REDUCE)) {
                checkJobCompletion(jobId);
            }
        } else if (status.equals("FAILED")) {
            String jobId = taskToJob.get(taskId);
            Job job = jobs.get(jobId);
            job.setStatus(Job.JobStatus.FAILED);
            System.err.println("Job failed: " + jobId + ", due to task: " + taskId);
        }
    }
    
    private void handleHeartbeat(HeartbeatMessage heartbeat) {
        String taskTrackerId = heartbeat.getTaskTrackerId();
        int port = heartbeat.getPort();
        taskTrackers.put(taskTrackerId, heartbeat);
        // 更新TaskTracker的端口映射
        taskTrackerPorts.put(taskTrackerId, port);
        System.out.println("Received heartbeat from " + taskTrackerId + 
                          ", port: " + port +
                          ", available slots: " + (heartbeat.hasAvailableSlot() ? "YES" : "NO") + 
                          ", tasks: " + heartbeat.getRunningTasksNum() + "/" + heartbeat.getMaxTasksNum());
        
        // 处理任务状态更新
        for (Map.Entry<String, String> entry : heartbeat.getTaskStatus().entrySet()) {
            String taskId = entry.getKey();
            String status = entry.getValue();
            // 只有在任务已完成时才更新状态
            if (status.equals("COMPLETED") && tasks.containsKey(taskId) && !completedTasks.contains(taskId)) {
                Task task = tasks.get(taskId);
                task.setStatus(Task.COMPLETED);
                completedTasks.add(taskId); // 添加到已完成任务集合
                // 创建一个完成状态的报告
                StatusReport taskReport = new StatusReport(
                    taskId,
                    "COMPLETED",
                    1.0,
                    "Task completed according to heartbeat"
                );
                taskStatus.put(taskId, taskReport);
                // System.out.println("Updated task status from heartbeat: " + taskId + " -> COMPLETED");
                // 检查是否需要调度后续任务
                String jobId = taskToJob.get(taskId);
                if (jobId != null && task.getType().equals(Task.MAP)) {
                    checkAndScheduleReduceTasks(jobId);
                } else if (jobId != null && task.getType().equals(Task.REDUCE)) {
                    checkJobCompletion(jobId);
                }
                // 任务完成后清理任务分配记录
                if (taskAssignments.containsKey(taskId)) {
                    taskAssignments.remove(taskId);
                }
            }
        }
        if (heartbeat.hasAvailableSlot()) {
            assignPendingTasks(taskTrackerId);
        }
    }
    

    // 为TaskTracker分配待处理的任务
    private void assignPendingTasks(String taskTrackerId) {
        // 优先分配Map任务，然后再分配Reduce任务
        List<Task> pendingTasks = getPendingTasks();
        HeartbeatMessage heartbeat = taskTrackers.get(taskTrackerId);
        int availableSlots = heartbeat.getMaxTasksNum() - heartbeat.getRunningTasksNum();
        
        for (Task task : pendingTasks) {
            if (availableSlots <= 0) {
                break; // 没有更多的可用槽位
            }
            // 如果任务未被分配且未完成，则分配给TaskTracker
            String taskId = task.getTaskId();
            if (!taskAssignments.containsKey(taskId) && !completedTasks.contains(taskId)) {
                try {
                    int port = getTaskTrackerPort(taskTrackerId);
                    // 设置任务分配信息
                    task.setAssignedTrackerId(taskTrackerId);
                    task.setStatus(Task.RUNNING);
                    NetworkUtils.sendObject(task, NetworkUtils.LOCALHOST, port);
                    // 记录任务分配情况
                    taskAssignments.put(taskId, taskTrackerId);
                    System.out.println("Assigned " + task.getType() + " task: " + taskId + " to TaskTracker at port " + port);
                    availableSlots--; // 减少可用槽位
                } catch (Exception e) {
                    System.err.println("Failed to assign task " + taskId + ": " + e.getMessage());
                }
            }
        }
    }
    
    // 获取所有待处理的任务
    private List<Task> getPendingTasks() {
        List<Task> pendingTasks = new ArrayList<>();
        
        // 首先考虑所有Map任务
        for (Task task : tasks.values()) {
            if (task.getType().equals(Task.MAP) && task.getStatus().equals(Task.PENDING) && !completedTasks.contains(task.getTaskId()) && !taskAssignments.containsKey(task.getTaskId())) {
                pendingTasks.add(task);
            }
        }
        // 如果所有Map任务已经分配或完成，再考虑Reduce任务
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
        // 检查所有Map任务是否都已完成
        boolean allMapTasksCompleted = true;
        for (String id : jobToTasks.get(jobId)) {
            Task t = tasks.get(id);
            if (t.getType().equals(Task.MAP) && !completedTasks.contains(id)) {
                if (!t.getStatus().equals(Task.COMPLETED)) {
                    allMapTasksCompleted = false;
                    break;
                }
            }
        }
        if (allMapTasksCompleted) {
            Job job = jobs.get(jobId);
            int numReducers = job.getNumReducers();
            String intermediateDir = job.getOutputPath() + "/intermediate";
            String outputDir = job.getOutputPath() + "/output";
            
            // 创建Reduce任务
            for (int i = 0; i < numReducers; i++) {
                String reduceTaskId = "reduce_" + jobId + "_" + i;
                // 如果任务已经存在，跳过创建
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
            for (String taskTrackerId : taskTrackers.keySet()) {
                HeartbeatMessage heartbeat = taskTrackers.get(taskTrackerId);
                if (heartbeat.hasAvailableSlot()) {
                    assignPendingTasks(taskTrackerId);
                }
            }
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
    // 获取一个可以执行任务TaskTracker
    private String getIdleTaskTracker() {
        for (Map.Entry<String, HeartbeatMessage> entry : taskTrackers.entrySet()) {
            // 使用HeartbeatMessage中的状态来判断TaskTracker是否空闲
            HeartbeatMessage report = entry.getValue();
            if (report.hasAvailableSlot()) {
                return entry.getKey();
            }
        }
        return null;
    }

    // 获取TaskTracker的端口号
    private int getTaskTrackerPort(String taskTrackerId) {
        Integer port = taskTrackerPorts.get(taskTrackerId);
        if (port == null) {
            System.err.println("No port mapping for TaskTracker ID: " + taskTrackerId);
            return -1;
        }
        return port;
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
