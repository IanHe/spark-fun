###Objective
####- Learn the anatomy of a Spark cluster
####- Start a Spark application directly on a cluster
<br></br>
![img of 001](imgs/topic2/001.png)
![img of 002](imgs/topic2/002.png)
![img of 003](imgs/topic2/003.png)
![img of 004](imgs/topic2/004.png)
<br></br>
####Add TestDeployApp.scala
####Add Jar in Artifact and build artifact jar file
![img of 005](imgs/topic2/005.png)
![img of 006](imgs/topic2/006.png)
![img of 007](imgs/topic2/007.png)
####In spark-cluster/docker-compose.yml, the ./app & ./data will be mounted to /opt/spark-app & /opt/spark-data
![img of 009](imgs/topic2/009.png)
####Copy out/artifacts/spark_essentials_fun_jar/spark-essentials-fun.jar to spark-cluster/apps
####Copy src/main/resources/data/movies.json to spark-cluster/data
####In the terminal, run: cd spark-cluster -> docker-compose up --scale spark-worker=3
![img of 008](imgs/topic2/008.png)
####In another terminal, run: docker exec -it spark-cluster_spark-master_1 bash
####the spark-apps contains spark-essentials-fun.jar and spark-data contains movies.json
![img of 010](imgs/topic2/010.png)
####cd /spark/bin and run: with --verbose to view all the running steps and information
![img of 011](imgs/topic2/011.png)
![img of 012](imgs/topic2/012.png)
####After a while, goodComedies are generated
