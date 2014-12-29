package Stripes_relativefreq;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,MapWritable,Text,FloatWritable> {

	private MapWritable result = new MapWritable();

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		result.clear();
		int totalCount = 0;	
		for (MapWritable val : values) {
			Set<Writable> keys= val.keySet(); 
			for(Entry<Writable,Writable>everyVal : val.entrySet()){
				if(result.containsKey(everyVal.getKey())){
					int finalCount =  Integer.parseInt(result.get(everyVal.getKey()).toString()) + Integer.parseInt(everyVal.getValue().toString());
					result.put(everyVal.getKey(),new IntWritable(finalCount));
					totalCount = totalCount+Integer.parseInt(everyVal.getValue().toString());
				}
				else{
					result.put(everyVal.getKey(),everyVal.getValue());
					totalCount=totalCount+Integer.parseInt(everyVal.getValue().toString());
				}

			}
		}
		
		String keyOfMap=key.toString();
		
		for(Entry<Writable, Writable> displayCoOccurence_Stripes_Stripes:result.entrySet())
		{
			String KeyToDisplay=keyOfMap+"_"+displayCoOccurence_Stripes_Stripes.getKey().toString();
			Float relativeFre = (Float.parseFloat(displayCoOccurence_Stripes_Stripes.getValue().toString()))/totalCount;
			context.write(new Text(KeyToDisplay), new FloatWritable(relativeFre));
		}
	
	}
}
