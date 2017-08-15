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
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.User;

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
        String status = tuple.getString(1);

        try {
            JSONObject jsonStatus = new JSONObject(status);

            User user = (User) jsonStatus.getJSONObject("user");
            String userScreenName = user.getScreenName();
            JSONObject entities = jsonStatus.getJSONObject("entities");
            JSONArray userMentions = entities.getJSONArray("user_mentions");

            collector.emit(new Values("mentions", userScreenName, userMentions));

        } catch (JSONException ex) {
            
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("mentions", "user", "userMentions"));
    }
}
