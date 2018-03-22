package com.superzhangx.mrdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Administrator on 2018/3/22.
 */
public class GetWorkCountTopN {
    public static class WordCountTopNMapper
            extends Mapper<Object,Text,Text,IntWritable> {

        private IntWritable wordCount = new IntWritable(1);
        private Text word = new Text();

        private int maxvalue = 0;

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String k = itr.nextToken().trim().toString();
            int v = Integer.parseInt(itr.nextToken().trim());


            if(maxvalue > 0) {
                if(v > maxvalue){
                    maxvalue = v;
                    word.set(k);
                    wordCount.set(maxvalue);
                }
            }else{
                maxvalue = v;
                word.set(k);
                wordCount.set(maxvalue);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                context.write(word, wordCount);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class WordCountTopNReduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private  IntWritable result = new IntWritable(1);
        private Text word = new Text();

        private int maxvalue = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException{

            int v = Integer.parseInt(values.iterator().next().toString());
            if(maxvalue > 0) {
                if(v > maxvalue){
                    maxvalue = v;
                    word.set(key.toString());
                }
            }else{
                maxvalue = v;
                word.set(key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            result.set(maxvalue);
            context.write(word, result);
        }
    }

    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();
        Job sortJob = Job.getInstance(conf, "top n job");
        sortJob.setJarByClass(GetWorkCountTopN.class);
        sortJob.setMapperClass(WordCountTopNMapper.class);
        sortJob.setReducerClass(WordCountTopNReduce.class);

        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(IntWritable.class);

        sortJob.setInputFormatClass(TextInputFormat.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);

        sortJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(sortJob, new Path("/zx_deploy/out/part-r-*"));
        FileOutputFormat.setOutputPath(sortJob, new Path("/zx_deploy/topn"));
        FileOutputFormat.setCompressOutput(sortJob,false); //设置输出是否压缩
        //FileOutputFormat.setOutputCompressorClass(sortJob, GzipCodec.class); //设置压缩格式

        System.out.println("started sort job .......");
        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

    }
}
