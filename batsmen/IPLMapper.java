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
		
		if((EachBall[0].equals(EachBall[4])) && (!(EachBall[5].equals("run out")))){
			output.collect(new Text( EachBall[0]), new Text(EachBall[1]));
		}
		
	}
}
