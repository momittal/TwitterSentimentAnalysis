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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

/**
 *
 * @author mohit
 */
public class Neo4JBolt extends BaseRichBolt {

    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;
    private Driver driver;
    private Session session;
    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {

        // save the collector for emitting tuples
        collector = outputCollector;
        driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "mohit" ) );
        // driver = GraphDatabase.driver( "bolt://localhost:7474", AuthTokens.basic( "neo4j", "mohit" ) );
        session = driver.session();

    }

    @Override
    public void execute(Tuple tuple) {
        // get the word from the 1st column of incoming tuple
        String user = (String) tuple.getValueByField("user");
        String userMentioned = (String) tuple.getValueByField("userMentions");
        
//        See if users exists or not -> if not then create users and then create relationship (relationships can be duplicated)
        String query = "MERGE (a:User {name: \"" + user + "\"}) MERGE (b:User { name : \"" +  userMentioned + "\" }) CREATE (a)-[r:Mentioned]->(b) RETURN a,type(r), b";
 
        session.run(query);
        collector.emit(new Values(""));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("mentions", "user", "userMentions"));
    }
}
