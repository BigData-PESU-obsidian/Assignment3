
package IPLanalyser;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class IPLDriver {
	public static void main(String[] args) {
		JobClient my_client = new JobClient();
		
		// Create a configuration object for the job
		JobConf job_conf = new JobConf(IPLDriver.class);
		JobConf sort_output = new JobConf(IPLDriver.class);

		// Set a name of the Job
		job_conf.setJobName("IPLanalyse");
		sort_output.setJobName("SortOutput");

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setMapOutputValueClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		sort_output.setOutputKeyClass(Text.class);
		sort_output.setMapOutputValueClass(Text.class);
		sort_output.setMapOutputKeyClass(IntWritable.class);
		sort_output.setOutputValueClass(IntWritable.class);

		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(IPLanalyser.IPLMapper.class);
		job_conf.setReducerClass(IPLanalyser.IPLReducer.class);

		sort_output.setMapperClass(IPLanalyser.IPLSortMapper.class);
		sort_output.setReducerClass(IPLanalyser.IPLSortReducer.class);
		
		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);
		job_conf.set("mapred.textoutputformat.separator", ",");

		sort_output.setInputFormat(TextInputFormat.class);
		sort_output.setOutputFormat(TextOutputFormat.class);
		sort_output.set("mapred.textoutputformat.separator", ",");

		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

		FileInputFormat.setInputPaths(sort_output, new Path(args[2]));
		FileOutputFormat.setOutputPath(sort_output, new Path(args[3]));
		
		my_client.setConf(job_conf);
		try {
			// Run the job 
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}

		my_client.setConf(sort_output);
		try {
			// Run the job 
			JobClient.runJob(sort_output);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
