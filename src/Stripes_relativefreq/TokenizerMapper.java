package Stripes_relativefreq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable>{

//	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private MapWritable occurrenceMap = new MapWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<String> AllTokens = new ArrayList<String>();
		
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String hashtag = itr.nextToken();
			if(hashtag.startsWith("#")){
				AllTokens.add(hashtag);
			}
			
		}
		Collections.sort(AllTokens);
		
		if (AllTokens.size() > 1) {
			for (int i = 0; i < AllTokens.size(); i++) {
				word.set(AllTokens.get(i));
				occurrenceMap.clear();
				for (int j = i+1; j < AllTokens.size(); j++) {
					Text neighbor = new Text(AllTokens.get(j));
					if(occurrenceMap.containsKey(neighbor)){
						IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
						count.set(count.get()+1);
					}else{
						occurrenceMap.put(neighbor,new IntWritable(1));
					}
				}
				context.write(word,occurrenceMap);
			}
		}
	}
}
