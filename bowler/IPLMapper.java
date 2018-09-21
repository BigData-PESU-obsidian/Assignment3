package IPLanalyser;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class IPLMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		// the input format is " batsman_name, bowler_name, batsman_runs, extras, player_out_name, kind "
		
		String valueString = value.toString();
		String[] EachBall = valueString.split(",");
		
		output.collect(new Text(EachBall[1]) , new Text( EachBall[0] +"_"+ Integer.parseInt(EachBall[2])));
		
	}
}
