
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
		sort_output.setJobName("SortOutput")

		// Specify data type of output key and value
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setMapOutputValueClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		job_conf.setOutputKeyClass(Text.class);
		job_conf.setMapOutputValueClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		// Specify names of Mapper and Reducer Class
		job_conf.setMapperClass(IPLanalyser.IPLMapper.class);
		job_conf.setReducerClass(IPLanalyser.IPLReducer.class);

		// Specify formats of the data type of Input and output
		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);
		job_conf.set("mapred.textoutputformat.separator", ",");

		// Set input and output directories using command line arguments, 
		//arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.
		
		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

		my_client.setConf(job_conf);
		try {
			// Run the job 
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class IntComparator extends WritableComparator {

		 public IntComparator() {
			 super(IntWritable.class);
		 }

		 @Override
		 public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			 
			 Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			 Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
			 
			 return v1.compareTo(v2) * (-1);
         }
	 }
}
