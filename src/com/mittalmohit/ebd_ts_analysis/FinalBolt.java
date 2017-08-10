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
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author mohit
 */
public class FinalBolt extends BaseRichBolt{
   

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    

    
  }

  @Override
  public void execute(Tuple tuple)
  {
    // access the first column 'word'
    String word = tuple.getStringByField("word");

    // access the second column 'count'
    Integer count = tuple.getIntegerByField("count");
    
    System.out.println(word + " --> " + count);
    
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}
