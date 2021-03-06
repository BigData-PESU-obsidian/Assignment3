package IPLanalyser;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class IPLSortReducer extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {

	public void reduce(IntWritable t_key, Iterator<Text> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		while(values.hasNext()){
			String[] combo = values.next().toString().split("_");
			output.collect(new Text("|Batsman: "+ combo[0] + "       | Bowler: "+ combo[1]), t_key);
		}
	}
}
