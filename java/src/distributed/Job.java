package distributed;

import java.io.Serializable;
import java.util.UUID;

// 将作业的描述信息封装成可序列化的Job类
public class Job implements Serializable {

    private String jobId;
    private String inputPath;
    private String outputPath;
    private String mapperClass;
    private String reducerClass;
    private String combinerClass;
    private int numReducers;
    private Integer numMapTasks; // 用户指定的Map任务数量（可选）
    private static final float REDUCE_START_THRESHOLD = 0.6f; // 60%的Map任务完成后启动Reduce
    public enum JobStatus {
        WAITING, RUNNING, COMPLETED, FAILED
    }
    private JobStatus status;



    // 不带Combiner和用户指定的Map任务数量
    public Job(String inputPath, String outputPath, String mapperClass, String reducerClass, int numReducers) {
        this.jobId = UUID.randomUUID().toString().substring(0, 8);
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        this.combinerClass = null;
        this.numReducers = numReducers;
        this.numMapTasks = null;
        this.status = JobStatus.WAITING;
    }
    
    // 带Combiner的构造函数
    public Job(String inputPath, String outputPath, String mapperClass, String reducerClass, String combinerClass, int numReducers) {
        this(inputPath, outputPath, mapperClass, reducerClass, numReducers);
        this.combinerClass = combinerClass;
    }
    
    // 带Map任务数量的构造函数
    public Job(String inputPath, String outputPath, String mapperClass, String reducerClass, int numReducers, Integer numMapTasks) {
        this(inputPath, outputPath, mapperClass, reducerClass, numReducers);
        this.numMapTasks = numMapTasks;
        System.out.println("^66666");
    }
    
    // 完整参数的构造函数
    public Job(String inputPath, String outputPath, String mapperClass, String reducerClass, String combinerClass, int numReducers, Integer numMapTasks) {
        this(inputPath, outputPath, mapperClass, reducerClass, combinerClass, numReducers);
        this.numMapTasks = numMapTasks;
    }
    
    public String getJobId() {
        return jobId;
    }
    
    public String getInputPath() {
        return inputPath;
    }
    
    public String getOutputPath() {
        return outputPath;
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
    
    public String getCombinerClass() {
        return combinerClass;
    }
    
    public boolean hasCombiner() {
        return combinerClass != null && !combinerClass.isEmpty();
    }
    
    public float getReduceStartThreshold() {
        return REDUCE_START_THRESHOLD;
    }
    
    public Integer getNumMapTasks() {
        return numMapTasks;
    }
    
    public boolean hasSpecifiedMapTasks() {
        return numMapTasks != null;
    }
    
    public JobStatus getStatus() { return status; }
    
    public void setStatus(JobStatus status) { this.status = status; }
    
    @Override
    public String toString() {
        return "Job[" + jobId + ", status=" + status + ", input=" + inputPath + ", output=" + outputPath + ", mapper=" + mapperClass + ", reducer=" + reducerClass + 
               (hasCombiner() ? ", combiner=" + combinerClass : "") + ", reducers=" + numReducers  +   (hasSpecifiedMapTasks() ? ", numMapTasks=" + numMapTasks : "") +
               ", reduceStartThreshold=" + (int)(REDUCE_START_THRESHOLD * 100) + "%" + "]";
    }
}
