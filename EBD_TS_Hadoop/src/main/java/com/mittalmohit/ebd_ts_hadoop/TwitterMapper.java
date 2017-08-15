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




public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable>{

	private URI[] files;
	private HashMap<String, String> AFINN_map = new HashMap<String, String>();

	
	public void setup(Context context) throws IOException
	{
		files = DistributedCache.getCacheFiles(context.getConfiguration());
		System.out.println("files:"+ files);
		Path path = new Path(files[0]);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line="";

		while((line = br.readLine()) != null)
		{
			String splits[] = line.split("\t");
			AFINN_map.put(splits[0], splits[1]);
		}
		br.close();
		in.close();
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			
		
				String tweet = value.toString();
				int sentiment_sum = 0;

				StringTokenizer tokenizer = new StringTokenizer(tweet);

	            while(tokenizer.hasMoreTokens()) {
	                String word = tokenizer.nextToken();
	            	
				
					if(word.length() > 1 && AFINN_map.containsKey(word))
					{
						Integer s = new Integer(AFINN_map.get(word));
						sentiment_sum += s;
					}
				}
				// String aboutProduct = "obj.get("asin").toString()";
				String aboutProduct = "twitterSentiment";
				context.write(new Text(aboutProduct), new IntWritable(sentiment_sum));
			
			
		
		

	}
	
}