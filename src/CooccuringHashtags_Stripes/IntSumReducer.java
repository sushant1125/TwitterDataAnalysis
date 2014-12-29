package CooccuringHashtags_Stripes;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class IntSumReducer 
extends Reducer<Text,MapWritable,Text,IntWritable> {

	private MapWritable result = new MapWritable();

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		result.clear();
	/*	for (MapWritable val : values) {
			Set<Writable> keys= val.keySet(); 
			for(Writable k:keys){
				IntWritable fromCount = (IntWritable) val.get(k);
				if(result.containsKey(k)){
					IntWritable count = (IntWritable) result.get(k);
					count.set(count.get() + fromCount.get());
				}else {
					result.put(k, fromCount);
				}

			}
		}*/
		
		for (MapWritable val : values) {
			Set<Writable> keys= val.keySet(); 
			for(Entry<Writable,Writable>everyVal : val.entrySet()){
				if(result.containsKey(everyVal.getKey())){
					int finalCount =  Integer.parseInt(result.get(everyVal.getKey()).toString()) + Integer.parseInt(everyVal.getValue().toString());
					result.put(everyVal.getKey(),new IntWritable(finalCount));
				}
				else{
					result.put(everyVal.getKey(),everyVal.getValue());
				}

			}
		}
		
		
		
		String keyOfMap=key.toString();
		for(Entry<Writable, Writable> displayCoOccurence:result.entrySet())
		{
			String KeyToDisplay=keyOfMap+"_"+displayCoOccurence.getKey().toString();

			context.write(new Text(KeyToDisplay), (IntWritable) displayCoOccurence.getValue());
		}
		
		
		
		
		
		
		
	/*	Set<Writable> keysInResult= result.keySet(); 
		Text fkey = new Text();
		final IntWritable one = new IntWritable(21);
		for (Writable keyfinal : keysInResult) {
			
			String finalKey = key.toString()+" "+keyfinal.toString();
			fkey.set(finalKey);
			context.write(fkey, );
		}	*/
		
	//	context.write(key, (IntWritable)result.get(key));

		
	}
}
