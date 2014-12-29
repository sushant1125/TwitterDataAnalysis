package CooccuringHashtags;

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
	private Text word; 
	String hashTag;
	
	WordPair wordpair = new WordPair();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<String> setOfHashTag = new ArrayList<String>();
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		//StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String hashtag = itr.nextToken();
			if(hashtag.startsWith("#")){
				setOfHashTag.add(hashtag);
			}
			
		}
		
		
		
		
		
		
		
//		for(String temp: setOfHashTag)
//		{
//			context.write(new Text(temp), one);
//		}
//		/*
		Collections.sort(setOfHashTag);
		
		for(int i=0;i<setOfHashTag.size();i++){
			for(int j=i+1;j<setOfHashTag.size();j++){
				word= new Text();
				word.set(setOfHashTag.get(i)+" "+setOfHashTag.get(j));
				context.write(word, one);
			}
		}
		
		
		/*for(int i=0;i<setOfHashTag.size();i++){
			for(int j=i+1;j<setOfHashTag.size();j++){
				System.out.println(setOfHashTag.get(i)+" "+setOfHashTag.get(j));
			}
		}*/
		
		
		
		
	/*	StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String hashtag = itr.nextToken();
			if(hashtag.contains("#")){
				AllTokens.add(hashtag);
			}
		}*/

	/*	Collections.sort(AllTokens);
		if (AllTokens.size() > 1) {
			for (int i = 0; i < AllTokens.size(); i++) {
				wordpair.setWord(AllTokens.get(i));
				for (int j = i+1; j < AllTokens.size(); j++) {
					wordpair.setNeighbour(AllTokens.get(j));
					context.write(wordpair,one);
				}
			}
		}*/
	}
}