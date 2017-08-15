/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis.bolt;

import com.mittalmohit.ebd_ts_analysis.StormTopology;
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
public class SinkTypeBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void execute(Tuple tuple) {
        String value = tuple.getString(0);



        if (value.equals("newTweet")){
            String tweet = tuple.getString(1);
            collector.emit(StormTopology.HDFS_STREAM, new Values(tweet));
        }
        else if (value.equals("word_count")){
            String word = tuple.getStringByField("word");
            Integer count = tuple.getIntegerByField("count");
            collector.emit(StormTopology.HDFS_WORD_COUNT,new Values(word,count));
        }else if (value.equals("mentions")){
            String user = tuple.getStringByField("user");
            
            String userMentions = tuple.getStringByField("userMentions");

            collector.emit(StormTopology.HDFS_MENTIONS,new Values(user,userMentions));
        }
        


        collector.ack(tuple);
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StormTopology.HDFS_STREAM, new Fields("content"));
        declarer.declareStream(StormTopology.HDFS_WORD_COUNT, new Fields("sinkType", "content"));
        declarer.declareStream(StormTopology.HDFS_MENTIONS, new Fields("user", "userMentions"));
    }
}
