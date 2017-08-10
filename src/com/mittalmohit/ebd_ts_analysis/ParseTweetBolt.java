/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author mohit
 */
public class ParseTweetBolt extends BaseRichBolt {

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // get the 1st column 'tweet' from tuple
        String tweet = tuple.getString(0);

        // provide the delimiters for splitting the tweet
        String delims = "[ .,?!]+";

        // now split the tweet into tokens
        String[] tokens = tweet.split(delims);

        // for each token/word, emit it
        for (String token : tokens) {
            collector.emit(new Values(token));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-word"));
    }
}
