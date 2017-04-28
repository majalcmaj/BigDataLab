import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

import javax.swing.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Anita on 2017-04-07.
 */
public class StackOverflow extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StackOverflow(), args);
        System.exit(res);
    }

    public int run (String[] args) throws Exception{
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
       Path tmpPath = new Path("/user/743ea2c9-d49f-4045-9ba4-161d2a7cb2be/Kozicka/tmp");
       // Path tmpPath = new Path("tmp");
        fs.delete(tmpPath, true);

     //   Path tmpPath2 = new Path("tmp2");
        Path tmpPath2 = new Path("/user/743ea2c9-d49f-4045-9ba4-161d2a7cb2be/Kozicka/tmp2");
        fs.delete(tmpPath2, true);

        Path outputPath = new Path(args[1]);
        fs.delete(outputPath, true);

        Job jobA = Job.getInstance(conf, "StackOverflow");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);

        jobA.setMapperClass(ParseMap.class);
        jobA.setReducerClass(ParseRed.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(StackOverflow.class);
       // jobA.setNumReduceTasks(4);
        jobA.setOutputFormatClass(TextOutputFormat.class);
        jobA.waitForCompletion(true);


        Job jobB = Job.getInstance(conf, "StackOverflow");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(Text.class);
       // jobB.setNumReduceTasks(4);
        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setReducerClass(StatsRed.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, tmpPath2);
        //FileOutputFormat.setOutputPath(jobB, outputPath);

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);
      //  jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(StackOverflow.class);
        jobB.waitForCompletion(true);


        Job jobC = Job.getInstance(conf, "StackOverflow");
        jobC.setOutputKeyClass(Text.class);
        jobC.setOutputValueClass(NullWritable.class);
        jobC.setMapOutputKeyClass(IntWritable.class);
        jobC.setMapOutputValueClass(Text.class);

        jobC.setReducerClass(SortRed.class);
        jobC.setNumReduceTasks(1);
        jobC.setSortComparatorClass(DescendingIntWritableComparator.class);

        FileInputFormat.setInputPaths(jobC, tmpPath2);
        FileOutputFormat.setOutputPath(jobC, outputPath);

        jobC.setInputFormatClass(KeyValueTextInputFormat.class);
        jobC.setOutputFormatClass(TextOutputFormat.class);

        jobC.setJarByClass(StackOverflow.class);

        return jobC.waitForCompletion(true) ? 0 : 1;
    }

    public static class ParseMap extends Mapper<Object, Text, Text, Text> {

        // data[0] - postType
        // data[1] - id
        // data[2] - solvedID
        // data[3] - parentID
        // data[4] - score
        // data[5] - tag

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = line.split(",");
            if (data[0]=="1")
                context.write(new Text(data[1]), new Text(data[0]+","+data[2]+","+data[5]));
            else
                context.write(new Text(data[3]), new Text(data[0]+","+"1"+","));
        }
    }

    public static class ParseRed extends Reducer<Text, Text, Text, Text> {

        // val[0] - postType
        // val[1] - solvedID
        // val[2] - tag
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tag = null;
            boolean isSolved = false;
            int answer = 0;

            for (Text text : values) {
                String val[] = text.toString().split(",");
                if (val[0] == "1") {            // jezeli jest pytaniem
                    tag = val[2];
                    if (val[1] != null)           // czy jest rozwiazanie
                        isSolved = true;
                    else
                        isSolved = false;
                } else if (val[0] == "2") {     // jezeli jest odpowiedzia
                    answer += 1;
                }
            }
            context.write(new Text(tag), new Text(isSolved+","+answer));
           /* if (tag!=null){
                context.write(new Text(tag), new Text(isSolved+","+answer));
            }*/
        }
    }

    public static class StatsRed extends Reducer<Text, Text, IntWritable, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumAnswSolved = 0;
            int sumSolved = 0;
            int sumUnsolved = 0;
            int sumAnswUnsolved = 0;
            boolean isSolved = false;
            for (Text text : values){
                String val[] = text.toString().split(",");
                isSolved = Boolean.parseBoolean(val[0]);
                if (isSolved == true) {   // rozwiazane
                    sumSolved += Integer.parseInt(val[1]);
                    sumAnswSolved++;
                }
                else if (isSolved == false) {   // nierozwiazane
                    sumUnsolved += Integer.parseInt(val[1]);
                    sumAnswUnsolved++;
                }
            }
            context.write(new IntWritable(sumSolved), new Text(key+","+sumSolved +","+ sumSolved/sumAnswSolved +","+ sumUnsolved +","+ sumUnsolved/sumAnswUnsolved));
        }
    }

    public static class SortRed extends Reducer <Text, Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values){
               // String val[] = text.toString().split(",");
               context.write(new Text( text.toString()), NullWritable.get());
            }
        }
    }

    public static class DescendingIntWritableComparator extends IntWritable.Comparator {
        protected DescendingIntWritableComparator() {
            super();
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -1 * super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}

