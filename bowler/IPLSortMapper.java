package IPLanalyser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class IPLSortMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

		// the input format is " batsman_name, bowler_name, batsman_runs, extras, player_out_name, kind "
		
		String valueString = value.toString();
		String[] EachBall = valueString.split(",");
		
		output.collect(new IntWritable(Integer.parseInt(EachBall[1])),  new Text(EachBall[0]));
		
	}
}
