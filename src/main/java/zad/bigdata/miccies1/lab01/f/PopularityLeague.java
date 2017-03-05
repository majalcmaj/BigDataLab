package zad.bigdata.miccies1.lab01.f;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import zad.bigdata.miccies1.lab01.common.HdfsReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Popularity League");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PopularityLeagueMap.class);
        job.setReducerClass(PopularityLeagueReduce.class);

        Configuration conf = this.getConf();
        String league = conf.get("league");
        job.getConfiguration().set("league", league);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(PopularityLeague.class);
        return job.waitForCompletion(true) ? 0 : 1; //Submit the job to the cluster and wait for it to finish
    }

    public static class PopularityLeagueMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        Map<Integer, Integer> leagueLinksCounts = new HashMap<Integer, Integer>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            String leaguePath = conf.get("league");
            String leagueItems = HdfsReader.readHDFSFile(leaguePath, context.getConfiguration());
            for(String item : leagueItems.split("\n")) {
                leagueLinksCounts.put(Integer.parseInt(item), 0);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] numbers = line.split(" ");
            IntWritable one = new IntWritable(1);
            for(int i = 1 ; i < numbers.length; i++) {
                int link = Integer.parseInt(numbers[i]);
                if(leagueLinksCounts.containsKey(link)) {
                    context.write(new IntWritable(link), one);
                }
            }
        }
    }

    public static class PopularityLeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        Map<Integer,Integer> leagueLinksCounts = new HashMap<Integer, Integer>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int occurrencesCount = 0;
            for(IntWritable count : values) {
                occurrencesCount += count.get();
            }
            leagueLinksCounts.put(key.get(), occurrencesCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Map.Entry<Integer, Integer> leagueEntry : leagueLinksCounts.entrySet()) {
                int league = 0;
                int currValue = leagueEntry.getValue();
                for(Map.Entry<Integer, Integer> otherEntry : leagueLinksCounts.entrySet()) {
                    if(otherEntry.getValue() < currValue)
                        league++;
                }
                context.write(new IntWritable(leagueEntry.getKey()), new IntWritable(league));
            }
        }
    }
}
