package distributed;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// TaskTracker发送给JobTracker的心跳消息
public class HeartbeatMessage implements Serializable {
    private String taskTrackerId;  // TaskTracker的ID
    private long timestamp;        // 消息发送时间戳
    private int runningTasksNum;   // 当前TaskTracker正在运行的任务数量
    private int maxTasksNum;       // 当前TaskTracker最多可运行任务数量
    private int port;              // TaskTracker的端口号
    private Map<String, String> taskStatus;  // 任务状态 taskId -> status
    
    public HeartbeatMessage(String taskTrackerId, int runningTasksNum, int maxTasksNum, int port) {
        this.taskTrackerId = taskTrackerId;
        this.timestamp = System.currentTimeMillis();
        this.runningTasksNum = runningTasksNum;
        this.maxTasksNum = maxTasksNum;
        this.port = port;
        this.taskStatus = new HashMap<>();
    }
    
    public void addTaskStatus(String taskId, String status) {
        taskStatus.put(taskId, status);
    }
    
    public String getTaskTrackerId() {
        return taskTrackerId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public int getRunningTasksNum() {
        return runningTasksNum;
    }
    
    public int getMaxTasksNum() {
        return maxTasksNum;
    }
    
    public boolean hasAvailableSlot() {
        return runningTasksNum < maxTasksNum;
    }
    
    public Map<String, String> getTaskStatus() {
        return taskStatus;
    }
    
    public int getPort() {
        return port;
    }
    
    @Override
    public String toString() {
        return "Heartbeat[" + taskTrackerId + ", running=" + runningTasksNum + "/" + maxTasksNum + ", tasks=" + taskStatus.size() + "]";
    }
}
