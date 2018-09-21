package IPLanalyser;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class IPLReducer extends MapReduceBase implements Reducer<Text, Text, Text, IntWritable> {

	public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		Text batsman = t_key;

		Map<String, Integer> wicketHolder = new HashMap<String, Integer>();
		int maxWickets = 0;
		String max_bowler = new String();
		
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			String value = (String) values.next().toString();

			if(wicketHolder.containsKey(value)){
				wicketHolder.put(value, wicketHolder.get(value) + 1);
			}
			else{
				wicketHolder.put(value, 1);
			}
		}

		// to get the bowler who had the most wickets against this batsman
		Map.Entry<String, Integer> maxEntry = null;
		for(Map.Entry<String, Integer> entry : wicketHolder.entrySet()){
			if(maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0){
				maxEntry = entry;
			}
		}

		output.collect(new Text(batsman.toString() +"_"+ maxEntry.getKey()), new IntWritable(maxEntry.getValue()));
	}
}
