/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zad.bigdata.miccies1.lab03.ex1;

import java.io.DataInput;
import java.io.DataOutput;
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
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import zad.bigdata.miccies1.lab01.b.TopTitles;

// >>> Don't Change
public class IdealizedPageRank extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IdealizedPageRank(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        int iterationsCount = Integer.parseInt(conf.get("K"));
        
        Path[] tmpPath = new Path[] { new Path("tmp1"), new Path("tmp2")};
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tmpPath[0], true);
        fs.delete(tmpPath[1], true);

        
        conf.setBoolean("IS_LAST", false);
        Job job = Job.getInstance(this.getConf(), "Idelized page Rank");
        job.setMapperClass(InitialReadMapper.class);
        FileInputFormat.setInputPaths(job, args[0]);
        while(--iterationsCount > 0) {
            FileOutputFormat.setOutputPath(job, tmpPath[iterationsCount % 2]);
            job.setReducerClass(PageRankReduce.class);
            job.setJarByClass(IdealizedPageRank.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DataPacket.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DataPacket.class);
            job.waitForCompletion(true);
            job = Job.getInstance(this.getConf(), "Idelized page Rank");
            FileInputFormat.setInputPaths(job, tmpPath[iterationsCount % 2]);
        }
        
        conf.setBoolean("IS_LAST", true);
        job.setReducerClass(PageRankReduce.class);
        job.setJarByClass(IdealizedPageRank.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataPacket.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataPacket.class);
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

// <<< Don't Change

    public class InitialReadMapper extends Mapper<Object, Text, Text, DataPacket> {

        int recordsCount;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            recordsCount = Integer.parseInt(conf.get("N"));
        }
        
        
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(" ");
            data[0] = data[0].replaceAll(":", "");
            InfoPacket info = new InfoPacket(Arrays.copyOfRange(data, 1, data.length));
            context.write(new Text(data[0]), new DataPacket(true, info));
            for(int i = 1 ; i < data.length ; i ++) {
                context.write(new Text(data[i]), new DataPacket(false, new ValuePacket((1.0d / recordsCount) * (1.0 / data.length -1))));
            }
        }
    }

    public static class PageRankReduce extends Reducer<Text, DataPacket, Text, DataPacket> {
        private boolean isLast = false;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();    
            isLast = Boolean.getBoolean(conf.get("IS_LAST"));
        }
        
        @Override
        protected void reduce(Text key, Iterable<DataPacket> values, Context context) throws IOException, InterruptedException {
            double newValue = 0.0;
            InfoPacket ip = null;
            for(DataPacket dp : values) {
                if(dp.isInfo) {
                    ip = (InfoPacket)dp.getPacket();
                }else {
                    newValue += ((ValuePacket)dp.getPacket()).getValue();
                }
            }
            double neighbourRank = newValue * 1.0d / ip.getNeighbours().length;
            for(String neighbourKey : ip.getNeighbours()) {
                context.write(new Text(neighbourKey), new DataPacket(false, new ValuePacket(neighbourRank)));
            }
            if(isLast) 
                context.write(key, new DataPacket(ip));
        }
    }
    
    public class DataPacket implements Writable {
        private boolean isInfo;

        public boolean isIsInfo() {
            return isInfo;
        }

        public DataPacket(boolean isInfo, Writable packet) {
            this.isInfo = isInfo;
            this.packet = packet;
        }
        
        private Writable packet;
        
        public Writable getPacket() { 
            return packet;
        }
        
        public void write(DataOutput d) throws IOException {
            d.writeBoolean(isInfo);
            packet.write(d);
        }

        public void readFields(DataInput di) throws IOException {
            this.isInfo = di.readBoolean();
            if(isInfo) {
                this.packet = new InfoPacket();
                this.packet.readFields(di);
            }else {
                this.packet = new InfoPacket();
                this.packet.readFields(di);
            }
        }
        
        @Override
        public String toString() {
            return packet.toString();
        }
    }
    
    public class InfoPacket implements Writable {

        public InfoPacket() {
        }

        public InfoPacket(String[] neighbours) {
            this.neighbours = neighbours;
        }
        
        private String[] neighbours;

        public String[] getNeighbours() {
            return neighbours;
        }
        
        public void write(DataOutput d) throws IOException {
            d.writeInt(neighbours.length);
            for(String n : neighbours) {
                d.writeUTF(n);
            }
        }

        public void readFields(DataInput di) throws IOException {
            neighbours = new String[di.readInt()];
            for(int i = 0 ; i < neighbours.length ; i ++) {
                di.readUTF();
            }
        }
        @Override
        public String toString() {
            return Arrays.toString(neighbours);
        }
    }
    
    public class ValuePacket implements Writable {

        public ValuePacket() {
        }

        public ValuePacket(double value) {
            this.value = value;
        }
        
        private double value;

        public double getValue() {
            return value;
        }
        
        public void write(DataOutput d) throws IOException {
            d.writeDouble(value);
        }

        public void readFields(DataInput di) throws IOException {
            value = di.readDouble();
        }
        
        @Override
        public String toString() {
            return ((Double)value).toString();
        }
    }
    
    

}