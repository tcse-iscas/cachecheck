# cachecheck

The tool of checking persist/unpersist misuse in Spark.

Four steps to use CacheCheck:

### 1. Instrument some code into Spark source code.
1. Replace the persist(newLevel: StorageLevel, allowOverride: Boolean) and unpersist(blocking: Boolean = true) functions in RDD.scala.
2. Add jobInfo(rdd: RDD[_]) and getJobNums() functions to SparkContext.scala. Replace 
    runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) =     U,
      partitions: Seq[Int],
      resultHandler: (Int, U) =     Unit)
    function in SparkContext.scala.

### 2. Create a "trace" directory in spark root path. 
The trace file and job info file will be in this dir.

### 3. Run the Spark application. 
Two files will created in trace directory. 
If you want to rerun the same example, please delete these two files first!

### 4. Run cachecheck.jar for detecting bugs.
The first argument is the path of trace directory. The second one is the name of the application.
>java -jar cachecheck.jar E:\\Workspaces\\idea\\spark-2.4.3\\trace ConnectedComponentsExample$

Finally, if you can see these information on screen, the tool runs successfully:

    Begin to read job & trace file.
    Read job & trace file done.
    Jobs:
    0,1,2,
    0,1,2,5,6,7,8,11,13,15,17,19,20,21,23,24,25,27,28,29,31,
    0,1,2,5,6,7,8,11,13,15,17,19,20,21,23,24,25,27,28,29,32,34,36,37,38,40,41,42,44,45,46,48,
    0,1,2,5,6,7,8,11,13,15,17,19,20,21,23,24,25,27,28,29,32,33,49,50,51,52,53,54,55,
    Actual trace:
    persist 2,job 0,persist 2,persist 11,persist 13,persist 11,persist 15,persist 21,persist 15,persist 29,job 1,persist 15,persist 32,persist 38,persist 32,persist 46,job 2,unpersist 29,unpersist 15,unpersist 21,unpersist 29,unpersist 46,unpersist 11,unpersist 13,job 3,
    Begin to calculate RDDs which should be persisted.
    RDDs should be persisted:
    2,15,25,29,32,
    Begin to generate correct sequence.
    Correct sequence:
    persist 2,job 0,persist 15,persist 25,persist 29,job 1,unpersist 2,persist 32,job 2,unpersist 29,unpersist 25,unpersist 15,job 3,unpersist 32,
    Begin to detect bugs by comparing correct sequence with actual sequence.
    Bug detection done!
    Bugs:
    Bug: [No unpersist] for RDD 25
    Bug: [Unnecessary persist] for RDD 38
    Bug: [No persist] for RDD 25
    Bug: [No unpersist] for RDD 32
    Bug: [No unpersist] for RDD 2
    Bug: [Unnecessary persist] for RDD 13
    Bug: [Unnecessary persist] for RDD 46
    Bug: [Unnecessary persist] for RDD 11
    Bug: [Unnecessary persist] for RDD 21
    Saving bug report to E:\\Workspaces\\idea\\spark-2.4.3\\trace\ConnectedComponentsExample$.report
    Finished.