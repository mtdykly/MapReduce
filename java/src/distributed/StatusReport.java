package distributed;

import java.io.Serializable;

public class StatusReport implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String taskId;  // 任务ID
    private String status;  // "STARTED", "RUNNING", "COMPLETED", "FAILED"
    private double progress;
    private String message;
    private long timestamp;  // 消息发送时间戳
    
    // 任务状态报告
    public StatusReport(String taskId, String status, double progress, String message) {
        this.taskId = taskId;
        this.status = status;
        this.progress = progress;
        this.message = message;
        this.timestamp = System.currentTimeMillis();
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public String getStatus() {
        return status;
    }
    
    public double getProgress() {
        return progress;
    }
    
    public String getMessage() {
        return message;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    
    @Override
    public String toString() {
        return "Task: " + taskId + ", Status: " + status ;

    }
}
