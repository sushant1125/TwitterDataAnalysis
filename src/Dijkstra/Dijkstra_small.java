package Dijkstra;

import java.util.Iterator;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
 
public class Dijkstra_small extends Configured implements Tool {
 
    public static String OUT = "outfile",IN = "inputlarger";
    //public static String IN = "inputlarger";
 
    public static class TheMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
 
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text word = new Text();
            String line = value.toString();
            String[] sp = line.split(" ");
            System.out.println(sp.length);
            String temp;
            int distanceadd = Integer.parseInt(sp[1]) + 1;
            System.out.println(distanceadd);
            String[] PointsTo = sp[2].split(":");
            for(int k=1;k< sp.length;k++)
            {
            	System.out.println(sp[k]);
            }
            for(int i=0; i<PointsTo.length; i++){
                word.set("VALUE "+distanceadd);
                System.out.println(PointsTo[i]+ " %%%%%  "+word);
                context.write(new LongWritable(Integer.parseInt(PointsTo[i])), word);
                word.clear();
            }
            word.set("VALUE "+sp[1]);
            context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
            System.out.println(sp[0]+ " sp0_word  "+word);
            word.clear();
            for(int k=1;k< sp.length;k++)
            {
            	System.out.println(sp[k]);
            }
            word.set("NODES "+sp[2]);
            context.write( new LongWritable( Integer.parseInt( sp[0] ) ), word );
            System.out.println(sp[0]+ " sp0_word  "+word);
            word.clear();
        }
    }
 
    public static class TheReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nodes = "";
            Text word = new Text();
            int lowest = 10009;
            for(int k=0;k< Integer.parseInt(key.toString());k++)
            {
            	System.out.println();
            }
            
            for (Text val : values) {
                String[] sp = val.toString().split(" ");
                System.out.println(val);
                if(sp[0].equalsIgnoreCase("NODES")){
                	System.out.println(key);
                    nodes = null;
                    
                    nodes = sp[1];
                    System.out.println(nodes);
                    
                }else if(sp[0].equalsIgnoreCase("VALUE")){
                	System.out.println(key);
                    int distance = Integer.parseInt(sp[1]);
                    lowest = Math.min(distance, lowest);
                    System.out.println(lowest);
                }
            }
            for(int k=1;k< Integer.parseInt(key.toString());k++)
            {
            	System.out.println();
            }
            word.set(lowest+" "+nodes);
            System.out.println(word);
            context.write(key, word);
            word.clear();
        }
    }
 
    public int run(String[] args) throws Exception {
        
        IN = "/input1/input-graph-small";
        OUT = "/output/dijkstra_output";
        String infile = IN;
        String outputfile = OUT + System.nanoTime();
        System.out.println(infile);
        boolean isdone = false;
        getConf().set("mapred.textoutputformat.separator", " ");
        int iteration=0;
        boolean success = false;
        boolean truthvalue=true;
        HashMap <Integer, Integer> map2 = new HashMap<Integer, Integer>();
        System.out.println(outputfile);
        while(isdone == false){
        	iteration++;
        	if(truthvalue==true)
            {
        		System.out.println("iteration "+iteration);
	            Job job = new Job(getConf());
	            job.setJarByClass(Dijkstra_large.class);
	            job.setJobName("Dijkstra");
	            job.setOutputKeyClass(LongWritable.class);
	            job.setOutputValueClass(Text.class);
	            job.setMapperClass(TheMapper.class);
	            job.setReducerClass(TheReducer.class);
	            job.setInputFormatClass(TextInputFormat.class);
	            job.setOutputFormatClass(TextOutputFormat.class);
	            FileInputFormat.addInputPath(job, new Path(infile));
	            FileOutputFormat.setOutputPath(job, new Path(outputfile));
	            success = job.waitForCompletion(true);
	            System.out.println("iteration "+iteration);
            }
            if(infile != IN){
                String indir = infile.replace("part-r-00000", "");
                Path ddir = new Path(indir);
                System.out.println(indir);
                FileSystem dfs = FileSystem.get(getConf());
                System.out.println(ddir);
                dfs.delete(ddir, true);
            }
            infile = outputfile+"/part-r-00000";
            
            System.out.println(infile);
            isdone = true;
            Path ofile = new Path(infile);
            System.out.println(outputfile);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(ofile)));
            System.out.println(ofile);
            outputfile = OUT + System.nanoTime();
            HashMap<Integer, Integer> map1 = new HashMap<Integer, Integer>();
            String line=br.readLine();
            while (line != null){
            	System.out.println(line);
                String[] sp = line.split(" ");
                for(int k=0;k< sp.length;k++)
                {
                	System.out.println(sp[k]);
                }
                int node = Integer.parseInt(sp[0]);
                System.out.println(node);
                int distance = Integer.parseInt(sp[1]);
                System.out.println(distance);
                map1.put(node, distance);
                line=br.readLine();
                System.out.println(line);
            }
            if(map2.isEmpty()){
            	System.out.println("in empty if");
                isdone = false;
            }else{
                Iterator<Integer> itr = map1.keySet().iterator();
                while(itr.hasNext()){
                    int key = itr.next();
                    System.out.println(key);
                    int val = map1.get(key);
                    System.out.println(val);
                    System.out.println(key+" "+val);
                    if(map2.get(key) != val){
                        isdone = false;
                        System.out.println("in map 2 if");
                    }
                }
            }
            System.out.println(isdone);
            if(isdone == false)
            {
                map2.putAll(map1);
            }
        }
        System.out.println("done");
        return success ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
    	System.out.println("in main");
        System.exit(ToolRunner.run(new Dijkstra_small(), args));
    }
}
