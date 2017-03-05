package zad.bigdata.miccies1.lab01.c;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import zad.bigdata.miccies1.lab01.common.ConstSizeRanking;
import zad.bigdata.miccies1.lab01.common.HdfsReader;

import java.io.IOException;
import java.util.*;

// >>> Don't Change
public class TopTitleStatistics extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopTitleStatistics(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Title Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(TitleCountMap.class);
        jobA.setReducerClass(TitleCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopTitleStatistics.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Titles");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(ConstSizeRanking.class);

        jobB.setMapperClass(TopTitleStatsMap.class);
        jobB.setReducerClass(TopTitleStatsReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopTitleStatistics.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

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
// <<< Don't Change

    public static class TitleCountMap extends Mapper<Object, Text, Text, IntWritable> {
        List<String> stopWords;
        String delimiters;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {

            Configuration conf = context.getConfiguration();

            String stopWordsPath = conf.get("stopwords");
            String delimitersPath = conf.get("delimiters");

            this.stopWords = Arrays.asList(HdfsReader.readHDFSFile(stopWordsPath, conf).split("\n"));
            this.delimiters = HdfsReader.readHDFSFile(delimitersPath, conf);
        }


        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, this.delimiters);
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken().trim().toLowerCase();
                if (!this.stopWords.contains(nextToken)) {
                    context.write(new Text(nextToken), new IntWritable(1));
                }
            }
        }
    }

    public static class TitleCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class TopTitleStatsMap extends Mapper<Text, Text, NullWritable, ConstSizeRanking> {
        Integer N;
        private ConstSizeRanking bestN;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            this.bestN = new ConstSizeRanking(this.N);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int itemCount = Integer.decode(value.toString());
            bestN.insertItem(itemCount, key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), bestN);
        }
    }

    public static class TopTitleStatsReduce extends Reducer<NullWritable, ConstSizeRanking, Text, IntWritable> {
        Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(NullWritable key, Iterable<ConstSizeRanking> bestNs, Context context) throws IOException, InterruptedException {
            Map<String, Integer> aggregator = new HashMap<String, Integer>();
            for(ConstSizeRanking csr : bestNs) {
                for(ConstSizeRanking.Item item : csr) {
                    int currCount = 0;
                    if(aggregator.containsKey(item.getValue())) {
                        currCount = aggregator.get(item.getValue());
                    }
                    aggregator.put(item.getValue(), item.getKey() + currCount);
                }
            }
            ConstSizeRanking totalBestN = new ConstSizeRanking(N);
            for( Map.Entry item : aggregator.entrySet()) {
                totalBestN.insertItem((Integer)item.getValue(), (String)item.getKey());
            }

            Integer sum = 0, mean, max = 0, min = Integer.MAX_VALUE, var = 0;

            for(ConstSizeRanking.Item item : totalBestN) {
                int wordCount = item.getKey();
                sum += wordCount;
                if(wordCount > max) {
                    max = wordCount;
                }

                if(wordCount < min) {
                    min = wordCount;
                }
            }
            mean = sum / totalBestN.size();

            for(ConstSizeRanking.Item item : totalBestN) {
                int deviation = mean - item.getKey();
                var += deviation * deviation;
            }

            var /= totalBestN.size();

            context.write(new Text("Mean"), new IntWritable(mean));
            context.write(new Text("Sum"), new IntWritable(sum));
            context.write(new Text("Min"), new IntWritable(min));
            context.write(new Text("Max"), new IntWritable(max));
            context.write(new Text("Var"), new IntWritable(var));
        }
    }

}

// >>> Don't Change
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
// <<< Don't Change