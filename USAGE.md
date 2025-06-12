# 分布式MapReduce框架使用指南

这是一个简单的分布式MapReduce框架，包含JobTracker（Master）和TaskTracker（Worker）组件，支持Map、Shuffle和Reduce三个阶段的任务执行。

## 架构

框架由以下主要组件组成：

1. **JobTracker**：负责作业调度和管理
2. **TaskTracker**：负责执行具体的Map和Reduce任务
3. **Client**：用于提交MapReduce作业

## 分布式特性

1. **心跳机制**：TaskTracker定期（每5秒）向JobTracker发送心跳消息，报告自身状态
2. **任务状态管理**：任务在执行过程中有四种状态：PENDING（等待执行）、RUNNING（执行中）、COMPLETED（已完成）和FAILED（失败）
3. **容错处理**：JobTracker通过心跳监控TaskTracker的状态，如果15秒内未收到心跳，则认为TaskTracker已经离线

## 运行流程

1. 首先启动JobTracker服务
2. 然后启动TaskTracker服务（可以启动多个TaskTracker实例）
3. 最后通过Client提交作业

## 如何运行

### 1. 编译代码

```bash
cd /home/apricity/workspace/MapReduce
javac -d target java/src/distributed/*.java java/src/distributed/utils/*.java java/src/client/*.java java/src/udf/*.java
```

### 2. 启动JobTracker

```bash
cd /home/apricity/workspace/MapReduce
java -cp target distributed.JobTracker [port]
```

例如，使用端口9001：
```bash
java -cp target distributed.JobTracker 9001
```

### 3. 启动TaskTracker

```bash
cd /home/apricity/workspace/MapReduce
java -cp target distributed.TaskTracker <taskTrackerPort>
```

例如，TaskTracker使用端口9002
```bash
java -cp target distributed.TaskTracker 9002
```

#### 启动多个TaskTracker

可以启动多个TaskTracker实例，每个使用不同的端口：

```bash
# 启动第一个TaskTracker
java -cp target distributed.TaskTracker 9002

# 启动第二个TaskTracker
java -cp target distributed.TaskTracker 9003

```

### 4. 提交WordCount作业

```bash
cd /home/apricity/workspace/MapReduce
java -cp target client.Client <inputPath> <outputPath> <mapperClass> <reducerClass> <numReducers> [combinerClass] [numMapTasks] [jobTrackerPort]
```

基本示例：
```bash
java -cp target client.Client input.txt output udf.WordCountMapper udf.WordCountReducer 3 
```

使用Combiner进行本地聚合：
```bash
java -cp target client.Client input.txt output udf.WordCountMapper udf.WordCountReducer 3 udf.WordCountCombiner
```

用户指定map任务数量：
```bash
java -cp target client.Client input.txt output udf.WordCountMapper udf.WordCountReducer 3 "" 5
```

Combiner+用户指定map任务数量：
```bash
java -cp target client.Client input.txt output udf.WordCountMapper udf.WordCountReducer 3 udf.WordCountCombiner 5
```

## 如何实现自定义的MapReduce程序

1. 创建一个实现`Mapper`接口的类
2. 创建一个实现`Reducer`接口的类
3. 使用Client提交作业，指定Mapper和Reducer类名

## WordCount示例

示例中已经实现了WordCount应用：

- `udf.WordCountMapper`：实现单词分割和计数
- `udf.WordCountReducer`：实现单词计数汇总

