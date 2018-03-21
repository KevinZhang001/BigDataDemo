package com.superzhangx.hdfsdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by SuperZhangx on 2018/3/20.
 * 文件压缩解压
 *
 * Hadoop支持的编码器
 * DEFLATE      |       org.apache.hadoop.io.compress.DefaultCodec
 * gzip         |       org.apache.hadoop.io.compress.GzipCodec
 * bzip         |       org.apache.hadoop.io.compress.BZip2Codec
 * Snappy       |       org.apache.hadoop.io.compress.SnappyCodec
 */
public class FileCompressAndUncompress  {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length < 4){
            System.err.println("Usage: <type--compress|uncompress> <inPath> <outPath> <codec>");
            System.exit(2);
        }

        if(otherArgs[0].equals("compress")){
            compress(otherArgs[1], otherArgs[2], otherArgs[3]);
        }else if(otherArgs[0].equals("uncompress")){
            uncompress(otherArgs[1], otherArgs[2], otherArgs[3]);
        }else{
            System.err.println("参数错误！！");
            System.exit(2);
        }
    }

    /**
     *
     * @param inPath
     * @param outPath
     * @param codec 压缩编码器
     */
    public static void compress(String inPath, String outPath, String codec)
            throws ClassNotFoundException, IOException {
        Class<?> codecClass = Class.forName(codec);
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codecc = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream in = fs.open(new Path(inPath));
        FSDataOutputStream outStream = fs.create(new Path(outPath));

        System.out.println("compress start !");
        CompressionOutputStream out = codecc.createOutputStream(outStream);
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        System.out.println("compress success !");
    }

    /**
     *
     * @param inPath
     * @param outPath
     * @param codec 解码器
     */
    public static void uncompress(String inPath, String outPath, String codec)
            throws ClassNotFoundException, IOException {
        Class<?> codecClass = Class
                .forName(codec);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        CompressionCodec codecc = (CompressionCodec) ReflectionUtils
                .newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs
                .open(new Path(inPath));
        InputStream in = codecc.createInputStream(inputStream);
        FSDataOutputStream out = fs.create(new Path(outPath));

        System.out.println("uncompress start !");
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
        System.out.println("uncompress success !");
    }
}
