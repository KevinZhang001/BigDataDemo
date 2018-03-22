package com.superzhangx.mrdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by SuperZhangx on 2018/3/19.
 * sort wordcount by value
 */
public class WordCountAndSorted {
    public static class WordCountSortMapper
            extends Mapper<Object,Text,IntWritable,Text> {

        private IntWritable wordCount = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                wordCount.set(Integer.parseInt(itr.nextToken().trim()));
                context.write(wordCount, word);
            }
        }
    }

    public static class WordCountSortReduce
            extends Reducer<IntWritable, Text, Text, IntWritable>{
        private  Text result = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            for(Text val : values){
                result.set(val.toString());
                context.write(result, key);
            }
        }
    }

    public static class IntKeyComparator extends WritableComparator {
        protected IntKeyComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static class KeySectionPartitioner<K,V> extends Partitioner<K, V> {
        public KeySectionPartitioner(){

        }

        @Override
        public int getPartition(K k, V v, int numReduceTasks) {
            /**
             * int值的hashcode还是自己本身的数值
             */
            int maxValue = 10000;
            int keySection = 0;
            // key值大于maxValue 并且numReduceTasks大于1个才需要分区，否则直接返回0
            if (numReduceTasks > 1 && k.hashCode() > maxValue) {
                int sectionValue = k.hashCode() / maxValue;
                keySection = sectionValue  / numReduceTasks;
            }
            return keySection;
        }
    }

    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        /**
         * wordcount job
         */
        Job job = Job.getInstance(new Configuration(), "word count job");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.WordCountReduce.class);
        job.setReducerClass(WordCount.WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setNumReduceTasks(1); //数据量允许的情况下可设置为1达到控制文件输出量的目的

        for(int i = 0; i < otherArgs.length - 1; i++){
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //FileOutputFormat.setCompressOutput(job,false); //设置输出是否压缩
        //FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); //设置压缩格式


        long startTime,endTime;
        startTime = System.currentTimeMillis();
        System.out.println("started word count job:  " + startTime);
        job.waitForCompletion(true);
        endTime = System.currentTimeMillis();
        System.out.println("End time: " + endTime + "   Total time: " + (endTime - startTime));

        /**
         * sort wordcount job
         */

        Job sortJob = Job.getInstance(conf, "sort job");
        sortJob.setJarByClass(WordCountAndSorted.class);
        sortJob.setMapperClass(WordCountSortMapper.class);
        sortJob.setReducerClass(WordCountSortReduce.class);
        sortJob.setSortComparatorClass(IntKeyComparator.class);
        sortJob.setPartitionerClass(KeySectionPartitioner.class);

        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        //sortJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(sortJob, new Path("/zx_deploy/out/part-r-*"));
        FileOutputFormat.setOutputPath(sortJob, new Path("/zx_deploy/sortout"));
        FileOutputFormat.setCompressOutput(sortJob,false); //设置输出是否压缩
        //FileOutputFormat.setOutputCompressorClass(sortJob, GzipCodec.class); //设置压缩格式

        System.out.println("started sort job .......");
        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

    }
}
