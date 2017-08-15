/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis.bolt;

import java.util.HashMap;
import java.util.Map;
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

/**
 *
 * @author mohit
 */
public class Neo4JBolt extends BaseRichBolt {
    
    
    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;


    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {

        // save the collector for emitting tuples
        collector = outputCollector;


    }

    @Override
    public void execute(Tuple tuple) {
        // get the word from the 1st column of incoming tuple
        String user = (String) tuple.getValueByField("user");
        String[] usersMentioned = null;
        JSONArray userMentions = (JSONArray) tuple.getValueByField("userMentions");

//        if length of array is greater than 0
        int numOfUsersMentioned = userMentions.length();
        if (numOfUsersMentioned > 0) {
            for (int i = 0; i < numOfUsersMentioned; i++) {
                try {
                    usersMentioned[i] = (String) ((JSONObject) userMentions.get(i)).getString("screen_name");
                    collector.emit(new Values("mentions", user, usersMentioned[i]));
                } catch (JSONException ex) {
                    
                }
            }

        }else{
            // collector.emit(new Values("mentions", user, ""))); 
        }


           
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("mentions", "user", "userMentions"));
    }
}
