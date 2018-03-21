package com.superzhangx.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Created by SuperZhangx on 2018/3/20.
 * 重命名文件
 */
public class FileRename {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 2){
            System.err.println("Usage: rename <in> [<in>...] <out>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(conf);
        boolean flag = fs.rename(new Path(otherArgs[0]), new Path(otherArgs[1]));
        if(flag){
            System.out.println("rename success!!");
        }
        else{
            System.out.println("rename failed!!");
        }
    }
}
