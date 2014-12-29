package Pair_relativefreq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text(); 
	String hashTag;
		
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<String> AllTokens = new ArrayList<String>();
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String hashtag = itr.nextToken();
			if(hashtag.startsWith("#")){
				AllTokens.add(hashtag);
			}
			
		}
		
		Collections.sort(AllTokens);
	/*	while(!AllTokens.isEmpty()){
			context.write(new Text(AllTokens.get(0)), new IntWritable(1));
			AllTokens.remove(0);
		}*/
		
		for(int i=0;i<AllTokens.size();i++){
			int count = 0;
			for(int j=i+1;j<AllTokens.size();j++){
				word.set(AllTokens.get(i)+" "+AllTokens.get(j));
				context.write(word, one);
				count++;
			}
			context.write(new Text(AllTokens.get(i)+" #*"), new IntWritable(count));
		}
	
	
	}
}