package distributed;

import java.io.Serializable;

// 最小任务单元
public class Task implements Serializable {
    public static final String MAP = "MAP";
    public static final String REDUCE = "REDUCE";
    
    // 任务状态常量
    public static final String PENDING = "PENDING";
    public static final String RUNNING = "RUNNING";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";
    
    private String taskId;
    private String type;  // MAP REDUCE
    private String inputPath;
    private String outputPath;
    private int partitionId;
    private String mapperClass;
    private String reducerClass;
    private int numReducers;
    private String status = PENDING;
    private String assignedTrackerId;  // 记录当前执行这个任务的TaskTracker
    
    // MAP Task
    public Task(String taskId, String inputPath, String outputPath, String mapperClass, 
                String reducerClass, int numReducers) {
        this.taskId = taskId;
        this.type = MAP;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        this.numReducers = numReducers;
    }
    
    // REDUCE Task
    public Task(String taskId, int partitionId, String inputPath, String outputPath, 
                String reducerClass) {
        this.taskId = taskId;
        this.type = REDUCE;
        this.partitionId = partitionId;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.reducerClass = reducerClass;
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public String getType() {
        return type;
    }
    
    public String getInputPath() {
        return inputPath;
    }
    
    public String getOutputPath() {
        return outputPath;
    }
    
    public int getPartitionId() {
        return partitionId;
    }
    
    public String getMapperClass() {
        return mapperClass;
    }
    
    public String getReducerClass() {
        return reducerClass;
    }
    
    public int getNumReducers() {
        return numReducers;
    }
    
    public String getStatus() {
        return status;
    }
    
    public void setStatus(String status) {
        this.status = status;
    }
    
    public String getAssignedTrackerId() {
        return assignedTrackerId;
    }
    
    public void setAssignedTrackerId(String assignedTrackerId) {
        this.assignedTrackerId = assignedTrackerId;
    }
    
    @Override
    public String toString() {
        String assignedInfo = assignedTrackerId != null ? ", assigned=" + assignedTrackerId : "";
        if (type.equals(MAP)) {
            return "MapTask[" + taskId  + assignedInfo + ", input=" + inputPath +  ", output=" + outputPath + ", mapper=" + mapperClass + "]";
        } else {
            return "ReduceTask[" + taskId  + assignedInfo + ", partition=" + partitionId +  ", input=" + inputPath + ", output=" + outputPath + ", reducer=" + reducerClass + "]";
        }
    }
}
