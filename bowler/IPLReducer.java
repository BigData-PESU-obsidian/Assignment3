package IPLanalyser;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class IPLReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		
		Text bowler = t_key;
		
		Map<String, Integer> runsHolder = new HashMap<String, Integer>();
		int maxRuns = 0;
		String max_batsman = new String();
		
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			String value_cat = (String) values.next().toString();
			String[] value_holder = value_cat.split("_");

			String value = value_holder[0];
			if(runsHolder.containsKey(value)){
				runsHolder.put(value, runsHolder.get(value) + Integer.parseInt(value_holder[1]));
			}
			else{
				runsHolder.put(value, Integer.parseInt(value_holder[1]));
			}
		}

		// to get the bowler who had the most wickets against this batsman
		Map.Entry<String, Integer> maxEntry = null;
		for(Map.Entry<String, Integer> entry : runsHolder.entrySet()){
			if(maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0){
				maxEntry = entry;
			}
		}

		output.collect(new Text(bowler.toString() +"_"+ maxEntry.getKey()), new IntWritable(maxEntry.getValue()));

	}
}
