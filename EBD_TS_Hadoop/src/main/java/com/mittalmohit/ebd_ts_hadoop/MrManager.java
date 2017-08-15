/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_hadoop;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MrManager extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        // TODO Auto-generated method stub
        ToolRunner.run(new MrManager(), args);

    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub

        // Create configuration
        Configuration conf = new Configuration(true);

        //Creating Distributed Cache
        DistributedCache.addCacheFile(new URI("/AFINN.txt"),conf);

        // Create job & Submitting job
        Job job = new Job(conf, "SentimentAnalysis");
        job.setJarByClass(MrManager.class);

        // Setup MapReduce
        job.setMapperClass(TwitterMapper.class);
        job.setReducerClass(TwitterReducer.class);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

}