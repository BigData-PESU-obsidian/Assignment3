# Assignment 3 
## MapReduce

### Requirements 
* Install Hadoop on your local machine ,as given [here](https://data-flair.training/blogs/install-hadoop-on-ubuntu/) . 
* You need to set the CLASSPATH environment variable as follows
```
export CLASSPATH="$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-<VERSION>.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-common-<VERSION>.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-<VERSION>.jar:$HADOOP_HOME/lib/*"
```
* clone this repository
```
git clone https://github.com/BigData-PESU-obsidian/Assignment3
```
* Edit the ``` Assignment3/batsmen/job1.sh ``` and ```Assignment3/bowler/job1.sh``` and set the $CLASSPATH accordingly

### Running the program

* Start hadoop
``` $ $HADOOP_HOME/sbin/start-all.sh```

* clean the output directories if any
```$ ./clean.sh```

* run the scripts
```$ ./job1.sh```