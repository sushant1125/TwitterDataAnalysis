package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.metrics.FSDatasetMBean;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import sample.WordCount;

public class kMeans extends Configured implements Tool {
	private static final transient Logger LOG = LoggerFactory.getLogger(WordCount.class);
	public static String OUT = "outfile";
    public static String IN = "inputlarger";
    public static String IN2 = "inputlarger";
 
    public static class TheMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
 
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
        	Text word = new Text();
            String line = value.toString();
            long median1, median2, median3;
            
            median1 = Integer.parseInt(context.getConfiguration().get("median_1"));
            median2 = Integer.parseInt(context.getConfiguration().get("median_2"));
            median3 = Integer.parseInt(context.getConfiguration().get("median_3"));
            
            String[] sp = line.split("/////");
            if(sp.length==1)
            sp = line.split(" ");
            String NumOfFollowers=sp[sp.length-1];
            NumOfFollowers=NumOfFollowers.trim();
            int intFollowers=Integer.parseInt(NumOfFollowers);
            System.out.println("median1: "+median1 + " median2: " + median2 + " median3: "+median3);
            if(Math.abs(intFollowers-median1)>=Math.abs(intFollowers-median2))
            {
            	if(Math.abs(intFollowers-median2)>Math.abs(intFollowers-median3))
            	{
            		context.write(new LongWritable(Integer.parseInt("3")), new LongWritable(intFollowers));
            	}
            	else
            	{
            		context.write(new LongWritable(Integer.parseInt("2")), new LongWritable(intFollowers));
            	}
            }
            else
            {
            	context.write(new LongWritable(Integer.parseInt("1")), new LongWritable(intFollowers));
            }
    	}
    }
    
    
    public static class TheReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    	private LongWritable result = new LongWritable();
    	
    	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
        {
        	
        	Float sum = (float) 0,count=(float) 0;
        	
    		for (LongWritable val : values) 
    		{
    			//context.write(key, val);
    			sum += val.get();
    			count++;
    			System.out.println("key: "+key+"          value:"+val);
    		}
    		System.out.println("Total count for key "+key+"  is : "+count);
    		
    		long tempres=(long) (sum/count);
    		result.set(tempres);
    		context.write(key, result);
        }
    }
    
    public int run(String[] args) throws Exception {
    	//Configuration conf = new Configuration();		
    	
    	Configuration conf = new Configuration();
    	conf.set("mapred.textoutputformat.separator", " ");
		//LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		//LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		/* Set the Input/Output Paths on HDFS */
	
		 IN = "/input2/tweets4.txt";
	     OUT = "/output2/kmeans";
	     IN2 = "/input2/tweets4.txt";
	     
	     String infile = IN;
	     String infile2 = IN2;
	     String outputfile = OUT + System.nanoTime();
	     long median1 = 23, median2 = 31, median3 = 38;
	     long count1=0,count2=0,count3=0;
	     
	     boolean isdone = false;
	     boolean success = false;
	 
	     HashMap <Integer, Integer> _map = new HashMap<Integer, Integer>();
	     int iteration=0;
	     while(isdone == false)
	     {
	    	 conf.set("median_1", ""+median1);
	    	 conf.set("median_2", ""+median2);
	    	 conf.set("median_3", ""+median3);
	    	 iteration++;
	    	 System.out.println(iteration+"   :"+median1+"  ***  "+median2+" ***  "+median3);
	    	 //infile = IN;
	    	 Job job = Job.getInstance(conf);
	    	 //deleteFolder(getConf(),outputfile);
	    	 
	         job.setJarByClass(kMeans.class);
	         job.setJobName("kMeans");
	         job.setJarByClass(kMeans.class);
	         job.setOutputKeyClass(LongWritable.class);
	         job.setOutputValueClass(LongWritable.class);
	         job.setMapOutputKeyClass(LongWritable.class);
	         job.setMapOutputValueClass(LongWritable.class);
	         job.setMapperClass(TheMapper.class);
	         job.setReducerClass(TheReducer.class);
	         job.setInputFormatClass(TextInputFormat.class);
	         job.setOutputFormatClass(TextOutputFormat.class);
	         FileInputFormat.addInputPath(job, new Path(infile2));
	         FileOutputFormat.setOutputPath(job, new Path(outputfile));
	 
	         success = job.waitForCompletion(true);
	         if(infile != IN)
	         {
	             String indir = infile.replace("part-r-00000", "");
	             Path ddir = new Path(indir);
	             FileSystem dfs = FileSystem.get(conf);
	             dfs.delete(ddir, true);
	         }
	           
	         infile = outputfile+"/part-r-00000";
	         outputfile = OUT + System.nanoTime();
	            
	            
	         isdone = true;//set the job to NOT run again!
	         Path ofile = new Path(infile);
	         FileSystem fs = FileSystem.get(new Configuration());
	         BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ofile)));
	         
	         HashMap<Integer, Integer> imap = new HashMap<Integer, Integer>();
	         String line=br.readLine();
	         while (line != null)
	         {
	         	 String[] sp = line.split(" ");
	             int group = Integer.parseInt(sp[0]);
	             int tempMedian = Integer.parseInt(sp[1]);
	             imap.put(group, tempMedian);
	             System.out.println(group+" @@@@ "+tempMedian);
	             int evenOdd=0;
	            	 if(group==1)
		             {
		            	 median1=tempMedian;
		             }
		             if(group==2)
		             {
		            	 median2=tempMedian;
		             }
		             if(group==3)
		             {
		            	 median3=tempMedian;
		             }
	             line=br.readLine();
	         }
	         if(_map.isEmpty()){
	             //first iteration... must do a second iteration regardless!
	             isdone = false;
	         } 
             else
             {
	         	Iterator<Integer> itr = imap.keySet().iterator();
	            while(itr.hasNext())
	            {
	            	int key = itr.next();
	                int val = imap.get(key);
	                System.out.println();
	                if(_map.get(key) != val)
	                {
	                 	isdone = false;
	                }
	             }
	         }
	         if(isdone == false)
	         {
	             _map.putAll(imap);
	         }
	        
	     }
         
	     return success ? 0 : 1;
    }
    
    private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
    
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new kMeans(), args));
    }
}
