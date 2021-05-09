###open a terminal
####run: cd spark-cluster
####run: docker-compose up --scale spark-worker=3
![image of 001](imgs/topic1/001.png)
<br></br>
###copy the spark cluster's name, open another terminal
####run: docker exec -it spark-cluster_spark-master_1 bash
####run: cd spark
####run: ./bin/spark-shell
####see: Spark context available as 'sc', Spark session available as 'spark'
####Spark Console will be available at: http://localhost:4040/jobs/
![image of 002](imgs/topic1/002.png)
<br></br>
![image of 003](imgs/topic1/003.png)
####the count job will be showing at the Spark Console
![image of 004](imgs/topic1/004.png)
####Click the description, will show Details for Job with completed Stages
####There are 8 tasks which is 8 partitions
![image of 005](imgs/topic1/005.png)
![image of 006](imgs/topic1/006.png)
![image of 007](imgs/topic1/007.png)
<br></br>
![image of 008](imgs/topic1/008.png)
####The DAG shows 2 elements in the stage
![image of 009](imgs/topic1/009.png)
<br></br>
![image of 010](imgs/topic1/010.png)
####The job has 2 stages, 31 tasks(partitions)
![image of 011](imgs/topic1/011.png)
####Job Details show 2 stages - Stage 2 & Stage 3 (Spark count stages from 0 since the first job)
#### - Stage 3 takes 23 tasks with Shuffle Read, Stage 2 takes 8 tasks with Shuffle Write
![image of 012](imgs/topic1/012.png)
<br></br>
####convert RDD to DF
![image of 013](imgs/topic1/013.png)
####The job is single task
![image of 014](imgs/topic1/014.png)
####The details show convert RDD to DF takes 4 Steps(parallelize -> scan -> wholeStageCodegen -> mapPartitionsInternal)
![image of 015](imgs/topic1/015.png)
<br></br>
####Create Dataset(with 2 steps)
![image of 016](imgs/topic1/016.png)
####Job Details show 2 steps in the stage(Spark organize steps behind the scene, user do not have control over this)
![image of 017](imgs/topic1/017.png)
<br></br>
####run physical plan to explain the steps and splits(partitions)
####a physical plan describes all the operations that spark will do to compute the dataset
####splits =8 means spark will do 8 partitions for this dataset
![image of 018](imgs/topic1/018.png)
<br></br>
####Lets creat a new dataset ds2 by using 5 steps
####repartition ds1 with 9 partition
####the physical plan shows: spark will do splits with 8 first then do => Exchange RoundRobinPartitioning(9), 9 is the new number of partitions
![image of 019](imgs/topic1/019.png)
<br></br>
####Lets do the repartition on ds2
![image of 020](imgs/topic1/020.png)
<br></br>
####define ds5 by using selectExpr on ds3
####(ds1)Range(...) => (ds2)repartition => (ds5)selectExpr
####'#11L' is the identifier of the column, '#28L' is the identifier of the new resulting column
![image of 021](imgs/topic1/021.png)
<br></br>
####lets do a join on ds5 with ds4
![image of 022](imgs/topic1/022.png)
<br></br>
####lets do an aggregate function on the joined 
![image of 023](imgs/topic1/023.png)
<br></br>
####If you want to see everything spark has planed before run the job, you can run sum.explain(true)
####Parsed Logical Plan =>  Analyzed Logical Plan => Optimized Logical Plan => Physical Plan
![image of 024](imgs/topic1/024.png)
<br></br>
![image of 025](imgs/topic1/025.png)
