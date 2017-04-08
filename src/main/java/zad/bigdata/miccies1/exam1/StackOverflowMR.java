package zad.bigdata.miccies1.exam1;


import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// >>> Don't Change
public class StackOverflowMR extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new StackOverflowMR(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("tmp");
        Path tmpPath2 = new Path("tmp2");
        fs.delete(tmpPath, true);
        fs.delete(tmpPath2, true);
        Path outputPath = new Path(args[1]);
        fs.delete(outputPath, true);

        Job jobA = Job.getInstance(conf, "StackOverflow");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(DataPacket.class);

        jobA.setMapperClass(InitialReadMapper.class);
        jobA.setReducerClass(StackOverflowReducer.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setOutputFormatClass(SequenceFileOutputFormat.class);
        jobA.setJarByClass(StackOverflowMR.class);
        jobA.setNumReduceTasks(4);
        jobA.waitForCompletion(true);
        Job jobB = Job.getInstance(conf, "StackOverflow");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(DataPacket.class);
        jobB.setNumReduceTasks(4);
        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(DataPacket.class);

        jobB.setReducerClass(CounterReducer.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, tmpPath2);

        jobB.setInputFormatClass(SequenceFileInputFormat.class);
        jobB.setOutputFormatClass(SequenceFileOutputFormat.class);

        jobB.setJarByClass(StackOverflowMR.class);
        jobB.waitForCompletion(true);

        Job jobC = Job.getInstance(conf, "StackOverflow");
        jobC.setOutputKeyClass(Text.class);
        jobC.setOutputValueClass(NullWritable.class);

        jobC.setMapOutputKeyClass(IntWritable.class);
        jobC.setMapOutputValueClass(DataPacket.class);

        jobC.setReducerClass(SortReducer.class);
        jobC.setNumReduceTasks(1);
        jobC.setSortComparatorClass(DescendingIntWritableComparator.class);
        FileInputFormat.setInputPaths(jobC, tmpPath2);
        FileOutputFormat.setOutputPath(jobC, outputPath);

        jobC.setInputFormatClass(SequenceFileInputFormat.class);
        jobC.setOutputFormatClass(TextOutputFormat.class);

        jobC.setJarByClass(StackOverflowMR.class);
        return jobC.waitForCompletion(true) ? 0 : 1;
    }

    // <<< Don't Change

    public static final int POST_TYPE_IDX = 0;
    public static final int ID_IDX = 1;
    public static final int ACCEPTED_ANSWER_IDX = 2;
    public static final int PARENT_ID_IDX = 3;
    public static final int SCORE_IDX = 4;
    public static final int TAG_IDX = 5;

    public static class InitialReadMapper extends Mapper<Object, Text, Text, DataPacket> {


        public InitialReadMapper() {
        }

        int recordsCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            recordsCount = Integer.parseInt(conf.get("N"));
        }


        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens[POST_TYPE_IDX].equals("1")) {
                Boolean isAccepted = !tokens[ACCEPTED_ANSWER_IDX].equals("");
                context.write(new Text(tokens[ID_IDX]), new DataPacket(DataPacket.Type.QUESTION, new QuestionData(tokens[TAG_IDX], isAccepted)));
            } else {
                context.write(new Text(tokens[PARENT_ID_IDX]), new DataPacket(DataPacket.Type.ANSWER, NullWritable.get()));
            }
        }
    }

    public static class StackOverflowReducer extends Reducer<Text, DataPacket, Text, DataPacket> {
        public StackOverflowReducer() {
        }

        @Override
        protected void reduce(Text key, Iterable<DataPacket> values, Context context) throws IOException, InterruptedException {
            String tag = null;
            int answersCount = 0;
            Boolean isSolved = null;
            for (DataPacket value : values) {
                switch (value.getType()) {
                    case QUESTION:
                        QuestionData questionInfo = (QuestionData) value.getPacket();
                        tag = questionInfo.getTag();
                        isSolved = questionInfo.isSolved();
                        break;
                    case ANSWER:
                        answersCount += 1;
                }
            }
            if (isSolved == null) {
                throw new RuntimeException("Question info not found");
            }
            context.write(new Text(tag), new DataPacket(isSolved ? DataPacket.Type.ANSWERED_QUESTION : DataPacket.Type.NOT_ANSWERED_QUESTION, new IntWritable(answersCount)));
        }
    }

    public static class CounterReducer extends Reducer<Text, DataPacket, IntWritable, DataPacket> {

        @Override
        protected void reduce(Text key, Iterable<DataPacket> values, Context context) throws IOException, InterruptedException {
            int solvedQuestionsCount = 0;
            int unSolvedQuestionsCount = 0;
            int answersOfSolved = 0;
            int answersOfUnsolved = 0;
            for (DataPacket dp : values) {
                int answersCount = ((IntWritable) dp.getPacket()).get();
                switch (dp.getType()) {
                    case ANSWERED_QUESTION:
                        answersOfSolved += answersCount;
                        solvedQuestionsCount++;
                        break;
                    case NOT_ANSWERED_QUESTION:
                        answersOfUnsolved += answersCount;
                        unSolvedQuestionsCount++;
                        break;
                }
            }
            context.write(new IntWritable(solvedQuestionsCount), new DataPacket(DataPacket.Type.QUESTION_STATS, new QuestionStats(key.toString(), solvedQuestionsCount, (double) answersOfSolved / solvedQuestionsCount, unSolvedQuestionsCount, (double) answersOfUnsolved / unSolvedQuestionsCount)));
        }
    }

    public static class SortReducer extends Reducer<IntWritable, DataPacket, Text, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<DataPacket> values, Context context) throws IOException, InterruptedException {
            for (DataPacket dp : values) {
                context.write(new Text(dp.getPacket().toString()), NullWritable.get());
            }
        }
    }

    public static class DataPacket implements Writable {
        public DataPacket() {
        }

        public enum Type {
            QUESTION,
            ANSWER,
            ANSWERED_QUESTION,
            NOT_ANSWERED_QUESTION,
            QUESTION_STATS
        }

        DataPacket.Type type;

        public DataPacket.Type getType() {
            return type;
        }

        public DataPacket(DataPacket.Type type, Writable packet) {
            this.type = type;
            this.packet = packet;
        }

        private Writable packet;

        public Writable getPacket() {
            return packet;
        }

        public void write(DataOutput d) throws IOException {
            d.writeInt(type.ordinal());
            packet.write(d);
        }

        public void readFields(DataInput di) throws IOException {
            DataPacket.Type[] types = DataPacket.Type.values();
            int index = di.readInt();
            if (!(index >= 0 && index < types.length))
                throw new RuntimeException(String.format("Type with index %d is not supported.", index));
            this.type = types[index];
            switch (type) {
                case QUESTION:
                    this.packet = new QuestionData();
                    break;
                case ANSWER:
                    this.packet = NullWritable.get();
                    break;
                case ANSWERED_QUESTION:
                case NOT_ANSWERED_QUESTION:
                    this.packet = new IntWritable();
                    break;
                case QUESTION_STATS:
                    this.packet = new QuestionStats();
                    break;
                default:
                    throw new RuntimeException(String.format("Type '%s' is not supported.", this.type.toString()));
            }
            this.packet.readFields(di);
        }

        @Override
        public String toString() {
            return type.toString() + " " + packet.toString();
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

    @Getter
    @Setter
    public static class QuestionData implements Writable {
        public QuestionData() {
        }

        public QuestionData(String tag, boolean isSolved) {
            this.tag = tag;
            this.isSolved = isSolved;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(tag);
            dataOutput.writeBoolean(isSolved);
        }

        public void readFields(DataInput dataInput) throws IOException {
            this.tag = dataInput.readUTF();
            this.isSolved = dataInput.readBoolean();
        }


        private String tag;
        private boolean isSolved;
    }

    @Getter
    @Setter
    public static class QuestionStats implements Writable {

        public QuestionStats() {
        }

        public QuestionStats(String tag, int solvedCount, double meanAnswersOnSolved, int unsolvedCount, double meanAnswersOnUnsolved) {
            this.tag = tag;
            this.solvedCount = solvedCount;
            this.meanAnswersOnSolved = meanAnswersOnSolved;
            this.unsolvedCount = unsolvedCount;
            this.meanAnswersOnUnsolved = meanAnswersOnUnsolved;
        }

        public void write(DataOutput d) throws IOException {
            d.writeUTF(tag);
            d.writeInt(solvedCount);
            d.writeDouble(meanAnswersOnSolved);
            d.writeInt(unsolvedCount);
            d.writeDouble(meanAnswersOnUnsolved);
        }

        public void readFields(DataInput di) throws IOException {
            tag = di.readUTF();
            solvedCount = di.readInt();
            meanAnswersOnSolved = di.readDouble();
            unsolvedCount = di.readInt();
            meanAnswersOnUnsolved = di.readDouble();
        }

        @Override
        public String toString() {
            return String.format("%s\t%d\t%.2f\t|\t%d\t%.2f", tag, solvedCount, meanAnswersOnSolved, unsolvedCount, meanAnswersOnUnsolved);
        }

        private String tag;
        private int solvedCount;
        private double meanAnswersOnSolved;
        private int unsolvedCount;
        private double meanAnswersOnUnsolved;
    }
}