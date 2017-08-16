/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis.bolt;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.JSONException;
import twitter4j.JSONObject;

/**
 *
 * @author mohit
 */
public class ParseTweetNeo4JBolt extends BaseRichBolt {

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
        String tweet = tuple.getString(1);
        String tweetText = "", user = "", tweetId = "";
        try {
            //        get tweet text from json string
            JSONObject tweetObj = new JSONObject(tweet);
            tweetText = tweetObj.getString("tweetText");
            user = tweetObj.getString("user");
        } catch (JSONException ex) {
            Logger.getLogger(ParseTweetBolt.class.getName()).log(Level.SEVERE, null, ex);
        }

        StringTokenizer tokenizer = new StringTokenizer(tweetText);

        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (token.startsWith("@")) {
                collector.emit(new Values("mentions", user, token));
            }

        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mentions", "user", "userMentions"));
    }
}
