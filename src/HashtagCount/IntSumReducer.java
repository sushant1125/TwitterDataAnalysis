package HashtagCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable result = new IntWritable();
	ArrayList<Integer> countArrayList = new ArrayList<Integer>();
	ArrayList<String> wordArrayList = new ArrayList<String>();

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		//result.set(sum);
		//context.write(key, result);
		
		countArrayList.add(sum);
		wordArrayList.add(key.toString());
	
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		while(!countArrayList.isEmpty()){
			int max = Collections.max(countArrayList);
			int maxIndex = countArrayList.indexOf(max);
			String maxWord = wordArrayList.get(maxIndex);
			
			countArrayList.remove(maxIndex);
			wordArrayList.remove(maxIndex);
			context.write(new Text(maxWord), new IntWritable(max));
			
		}
	}
	
}
