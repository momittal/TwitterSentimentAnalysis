/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis.bolt;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 *
 * @author mohit
 */
public class FinalBolt extends BaseRichBolt{
   
  // place holder to keep the connection to redis
  transient RedisConnection<String,String> redis;

  
  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    
    // instantiate a redis connection
    RedisClient client = new RedisClient("localhost",6379);

    // initiate the actual connection
    redis = client.connect();
    
  }

  @Override
  public void execute(Tuple tuple)
  {
      String word = tuple.getStringByField("word");
      Integer count = tuple.getIntegerByField("count");

      // publish the word count to redis using word as the key
      redis.publish("TwitterWordCount", word + "|" + Long.toString(count));
    
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
