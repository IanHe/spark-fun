###open a terminal
####run: cd spark-cluster
####run: docker-compose up --scale spark-worker=3
![image of 001](imgs/001.png)
<br />
###copy the spark cluster's name, open another terminal
####run: docker exec -it spark-cluster_spark-master_1 bash
####run: cd spark
####run: ./bin/spark-shell
####see: Spark context available as 'sc', Spark session available as 'spark'
####Spark Console will be available at: http://localhost:4040/jobs/
![image of 002](imgs/002.png)
<br />
![image of 003](imgs/003.png)
####the count job will be showing at the Spark Console
![image of 004](imgs/004.png)
####Click the description, will show Details for Job with completed Stages
####There are 8 tasks which is 8 partitions
![image of 005](imgs/005.png)
![image of 006](imgs/006.png)
![image of 007](imgs/007.png)
<br />
![image of 008](imgs/008.png)
####The DAG shows 2 elements in the stage
![image of 009](imgs/009.png)
<br />
![image of 010](imgs/010.png)
####The job has 2 stages, 31 tasks(partitions)
![image of 011](imgs/011.png)
####Job Details show 2 stages - Stage 2 & Stage 3 (Spark count stages from 0 since the first job)
#### - Stage 3 takes 23 tasks with Shuffle Read, Stage 2 takes 8 tasks with Shuffle Write
![image of 012](imgs/012.png)
<br />
####convert RDD to DF
![image of 013](imgs/013.png)
####The job is single task
![image of 014](imgs/014.png)
####The details show convert RDD to DF takes 4 Steps(parallelize -> scan -> wholeStageCodegen -> mapPartitionsInternal)
![image of 015](imgs/015.png)
<br />
####Create Dataset
![image of 016](imgs/016.png)
####Job Details show 2 steps in the stage(Spark organize steps behind the scene, user do not have control over this)
![image of 017](imgs/017.png)
####run physical plan to explain the steps and splits(partitions)
![image of 018](imgs/018.png)