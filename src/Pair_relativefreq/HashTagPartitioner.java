package Pair_relativefreq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class HashTagPartitioner extends Partitioner<Text,IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int arg2) {
		//97-109 && 65-77
		String word = key.toString();
		int i = word.charAt(1);
		if(i>97 && i<=109 || i>65 && i<77)
			return 0;
		else
			return 1;
		
	}

	

	

	

	

}
