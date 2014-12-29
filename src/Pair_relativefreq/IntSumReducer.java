package Pair_relativefreq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,IntWritable,Text,FloatWritable> {
	HashMap<String, Float> relativeFreq = new HashMap<String,Float>();
	private IntWritable result = new IntWritable();
	int totalCount = 0;
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;

		for (IntWritable val : values) {
			sum += val.get();
		}

		if(key.toString().contains("*")){
			totalCount = sum;
		}
		else{
			String[] pair = key.toString().split(" ");
			float temp =((float)sum/(float)totalCount);
			context.write(new Text(pair[0]+" "+pair[1]), new FloatWritable(temp));
		}
	}
}
