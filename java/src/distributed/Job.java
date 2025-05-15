package distributed;

import java.io.Serializable;
import java.util.UUID;


public class Job implements Serializable {
    private String jobId;
    private String inputPath;
    private String outputPath;
    private String mapperClass;
    private String reducerClass;
    private int numReducers;
    public enum JobStatus {
        WAITING, RUNNING, COMPLETED, FAILED
    }
    private JobStatus status;

    public Job(String inputPath, String outputPath, String mapperClass, String reducerClass, int numReducers) {
        this.jobId = UUID.randomUUID().toString().substring(0, 8);
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        this.numReducers = numReducers;
        this.status = JobStatus.WAITING;
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
    
    public JobStatus getStatus() { return status; }
    
    public void setStatus(JobStatus status) { this.status = status; }
    
    @Override
    public String toString() {
        return "Job[" + jobId + ", status=" + status + ", input=" + inputPath + ", output=" + outputPath + ", mapper=" + mapperClass + 
               ", reducer=" + reducerClass + ", reducers=" + numReducers + "]";
    }
}
