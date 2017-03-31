import java.io.IOException;
import java.lang.Integer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class WHTest {
	
	public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }
	
    public static class WHTestMap extends Mapper<Object, Text, Text, IntWritable> {
       // List<String> commonWords = Arrays.asList("the", "a", "an", "and", "of", "to", "in", "am", "is", "are", "at", "not");
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
        	String delims = ",";
        	String[]tokens = line.split(delims);
            context.write(new Text(tokens[0]+" "+tokens[1]+", "+tokens[19]+" "+tokens[20]), new IntWritable(1));
        }
    }

    public static class WHTestReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    
    public static class TopWHMap extends Mapper <Text, Text, NullWritable, TextArrayWritable>{


		private TreeSet<Pair<Integer, String>> countPairMap = new TreeSet<Pair<Integer, String>>();
		
		@Override
    	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
    		Integer count = Integer.parseInt(value.toString());
    		String pair = key.toString();
    		
    		countPairMap.add(new Pair<Integer,String>(count,pair));
    		
    		if (countPairMap.size()>10){
    			countPairMap.remove(countPairMap.first());
    		}
    	}
    	
    	@Override
		protected void cleanup(Mapper<Text, Text, NullWritable, TextArrayWritable>.Context context)
				throws IOException, InterruptedException {
			for (Pair<Integer,String>item : countPairMap){
				String[] strings = {item.second, item.first.toString()};
				TextArrayWritable value = new TextArrayWritable(strings);
				context.write(NullWritable.get(), value);
			}
		}
    }
    
    private static class TopWHReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable>{

    	private TreeSet<Pair<Integer, String>> countPairMap = new TreeSet<Pair<Integer, String>>();
    	
		@Override
		protected void reduce(NullWritable key, Iterable<TextArrayWritable> values,
				Reducer<NullWritable, TextArrayWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException{
			for (TextArrayWritable val : values){
				Text[]line = (Text[]) val.toArray();
				
				String pair = line[0].toString();
				Integer count = Integer.parseInt(line[1].toString());
				
				countPairMap.add(new Pair<Integer, String>(count,pair));
				
				if (countPairMap.size()>10){
					countPairMap.remove(countPairMap.first());
				}
			}
			for (Pair<Integer,String>item:countPairMap){
				Text newPair = new Text(item.second);
				IntWritable value= new IntWritable(item.first);
				context.write(newPair,value);
			}
		}
    }
    

    public static void main(String[] args) throws Exception {
    	//The purpose of the driver is to orchestrate the jobs.
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);
    	Path tmpPath = new Path("tmp");
    	fs.delete(tmpPath, true);
    	
        Job jobA = Job.getInstance(conf, "WHTest");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(WHTestMap.class);
        jobA.setReducerClass(WHTestReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(WHTest.class);
        jobA.waitForCompletion(true);
        
        Job jobB = Job.getInstance(conf, "Top WH");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);
        
        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);
        
        jobB.setMapperClass(TopWHMap.class);
        jobB.setReducerClass(TopWHReduce.class);
        jobB.setNumReduceTasks(1);
        
        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));
        
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
        
        jobB.setJarByClass(WHTest.class);
        
        System.exit(jobB.waitForCompletion(true) ? 0 : 1); //Submit the job to the cluster and wait for it to finish
    }
}
    
    class Pair<A extends Comparable<? super A>,
    B extends Comparable<? super B>>
    implements Comparable<Pair<A, B>> {

public final A first;
public final B second;

public Pair(A first, B second) {
    this.first = first;
    this.second = second;
}

public static <A extends Comparable<? super A>,
        B extends Comparable<? super B>>
Pair<A, B> of(A first, B second) {
    return new Pair<A, B>(first, second);
}


public int compareTo(Pair<A, B> o) {
    int cmp = o == null ? 1 : (this.first).compareTo(o.first);
    return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
}

@Override
public int hashCode() {
    return 31 * hashcode(first) + hashcode(second);
}

private static int hashcode(Object o) {
    return o == null ? 0 : o.hashCode();
}

@Override
public boolean equals(Object obj) {
    if (!(obj instanceof Pair))
        return false;
    if (this == obj)
        return true;
    return equal(first, ((Pair<?, ?>) obj).first)
            && equal(second, ((Pair<?, ?>) obj).second);
}

private boolean equal(Object o1, Object o2) {
    return o1 == o2 || (o1 != null && o1.equals(o2));
}

@Override
public String toString() {
    return "(" + first + ", " + second + ')';
}
}
