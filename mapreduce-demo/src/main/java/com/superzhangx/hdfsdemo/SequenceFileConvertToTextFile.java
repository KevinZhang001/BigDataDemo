package com.superzhangx.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Created by SuperZhangx on 2018/3/21.
 * SequenceFile convert to TextFile
 */
public class SequenceFileConvertToTextFile {
    public static void ConvertToTextFile(String pathStr, String outPath)
            throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(pathStr);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        FSDataOutputStream out = fs.create(new Path(outPath));

        try {
            WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            Text t = new Text();
            while (reader.next(key, value)) {
                //System.out.printf("%s\t%s\n", key, value);
                out.write(value.toString().getBytes());
            }
        }finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(out);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Usage: <inPath> <outPath>");
            System.exit(2);
        }
        ConvertToTextFile(otherArgs[0], otherArgs[1]);
    }
}
