export CLASSPATH=$CLASSPATH:"/home/yashas/Documents/Work/Assignment/BD/Assignment3/batsmen/IPLanalyser/*";
javac -d . IPLMapper.java IPLReducer.java IPLDriver.java IPLSortMapper.java IPLSortReducer.java;
jar cfm IPL.jar Manifest.txt IPLanalyser/*.class;
hadoop jar IPL.jar /Assignment3/input_files_1 /Assignment3/output_of_batsmen_job_1 /Assignment3/output_of_batsmen_job_1/part-00000 /Assignment3/output_of_batsmen_job_2;
hdfs dfs -cat /Assignment3/output_of_batsmen_job_2/part-00000;
