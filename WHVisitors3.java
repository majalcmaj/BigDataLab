import java.io.IOException;
import java.lang.Integer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.curator.utils.DefaultZookeeperFactory;
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




public class WHVisitors {
	
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
	
    public static class WHTestMap extends Mapper<Object, Text, Text, NullWritable> {
    	Random r = new Random();
    	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
        	String delims = ",";
        	String[]tokens = line.split(delims);
        	if(tokens[11].equals("") || tokens[6].equals("") || r.nextDouble() < 0.1) 
        		context.write(value, NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
    	//The purpose of the driver is to orchestrate the jobs.
    	
    	 Job job = Job.getInstance(new Configuration(), "Wordcount");
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(LongWritable.class);

         job.setMapperClass(WHTestMap.class);
         job.setNumReduceTasks(1);

         FileInputFormat.setInputPaths(job, new Path(args[0]));
         FileOutputFormat.setOutputPath(job, new Path(args[1]));

         job.setJarByClass(WHVisitors.class);
         System.exit(job.waitForCompletion(true) ? 0 : 1);
         
    }
}
    