/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private URI[] files;
    private HashMap<String, String> AFINN_map = new HashMap<String, String>();

    public void setup(Context context) throws IOException {
        files = DistributedCache.getCacheFiles(context.getConfiguration());
        System.out.println("files:" + files);
        Path path = new Path(files[0]);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = "";

        while ((line = br.readLine()) != null) {
            String splits[] = line.split("\t");
            AFINN_map.put(splits[0], splits[1]);
        }
        br.close();
        in.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String tweet = value.toString();
        String tweetText = "null";
        String user = "null";
        int sentiment_sum = 0;

        try {
            //        get tweet text from json string
            JSONParser parser = new JSONParser();
            JSONObject tweetObj = (JSONObject) parser.parse(tweet);
            tweetText = (String) tweetObj.get("tweetText");
            user = (String) tweetObj.get("user");

            // System.out.println("mohit -- " + user + " -- " + tweetText);

            StringTokenizer tokenizer = new StringTokenizer(tweetText);

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();

                if (word.length() > 1 && AFINN_map.containsKey(word)) {
                    Integer s = new Integer(AFINN_map.get(word));
                    sentiment_sum += s;
                }
            }
        context.write(new Text(user), new IntWritable(sentiment_sum));

        } catch (Exception ex) {
        }





    }

}
