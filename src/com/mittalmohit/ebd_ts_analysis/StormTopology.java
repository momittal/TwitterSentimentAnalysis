/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis;

import com.mittalmohit.ebd_ts_analysis.spout.TweetSpout;
import com.mittalmohit.ebd_ts_analysis.bolt.ParseTweetBolt;
import com.mittalmohit.ebd_ts_analysis.bolt.WordCountBolt;
import com.mittalmohit.ebd_ts_analysis.bolt.SinkTypeBolt;
import com.mittalmohit.ebd_ts_analysis.bolt.BoltBuilder;
import com.mittalmohit.ebd_ts_analysis.bolt.FinalBolt;

import com.mittalmohit.ebd_ts_analysis.bolt.ParseTweetNeo4JBolt;
import com.mittalmohit.ebd_ts_analysis.bolt.Neo4JBolt;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 *
 * @author mohit
 */
public class StormTopology {

    public static final String HDFS_STREAM = "hdfs-stream-full";
    public static final String HDFS_WORD_COUNT = "hdfs-word-count";
    public static final String HDFS_MENTIONS = "hdfs-mentions";

    public StormTopology(String configFile) throws Exception {

    }

    public static void main(String[] args) throws Exception {
//      Create storm topology
        TopologyBuilder builder = new TopologyBuilder();
//        Create tweet spout
        String customerKey, secretKey, accessToken, accessSecret;
        customerKey = "ixvFfqeSO0iIBK9oiAhfAD7yN";
        secretKey = "XvbaPmZSrkM2QFgGWI4ZnsbIwqXHXfzS5zNs3wWvqA95lldVEN";
        accessToken = "100916947-5kLhqaUmB9Icl9eEBdgSjgZPxVH0u98MYT4OqPcu";
        accessSecret = "IY0JJYDCIriPIJq2xwJlCunaVb48qsa74Uu4P8A6aDHfU";
        TweetSpout tweetSpout = new TweetSpout(customerKey, secretKey, accessToken, accessSecret);

//        Create Bolts
        ParseTweetBolt parseTweetBolt = new ParseTweetBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        FinalBolt finalBolt = new FinalBolt();

        BoltBuilder boltBuilder = new BoltBuilder();
        SinkTypeBolt sinkTypeBolt = boltBuilder.buildSinkTypeBolt();
        HdfsBolt hdfsBolt = boltBuilder.buildHdfsBolt();
        SinkTypeBolt sinkTypeBolt2 = boltBuilder.buildSinkTypeBolt();
        HdfsBolt hdfsBolt2 = boltBuilder.buildWordCountHdfsBolt();
        ParseTweetNeo4JBolt parseNeo4JBolt = new ParseTweetNeo4JBolt();
        Neo4JBolt neo4JBolt = new Neo4JBolt();
        // HdfsBolt tweetTexthdfsBolt = boltBuilder.buildHdfsBolt();
        

//        Build Topology
        builder.setSpout("tweet-spout", tweetSpout, 1);
//        Save to hdfs
        builder.setBolt("sink-type-bolt", sinkTypeBolt, 1).shuffleGrouping("tweet-spout");
        builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("sink-type-bolt", HDFS_STREAM);

//        Word count //hashtag count
        builder.setBolt("parse-tweet-bolt", parseTweetBolt, 10).shuffleGrouping("tweet-spout");
        builder.setBolt("word-count-bolt", wordCountBolt, 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
        builder.setBolt("final-bolt", finalBolt, 1).globalGrouping("word-count-bolt");

        builder.setBolt("sink-type-bolt-2", sinkTypeBolt2, 1).globalGrouping("word-count-bolt");
        builder.setBolt("hdfs-bolt-2", hdfsBolt2, 1).shuffleGrouping("sink-type-bolt-2", HDFS_WORD_COUNT);
        

         builder.setBolt("parse-neo4J-bolt", parseNeo4JBolt, 10).shuffleGrouping("tweet-spout");
         builder.setBolt("neo4J-bolt", neo4JBolt, 1).shuffleGrouping("parse-neo4J-bolt");

//      Create Default Config Object
        Config conf = new Config();
//      Set cong in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {
//            run the topology in live cluster

        } else {
//          run the topology in simulated local cluster
            conf.setMaxTaskParallelism(3);
//          Create new local cluster
            LocalCluster cluster = new LocalCluster();

//          submit topology to local cluster
            cluster.submitTopology("StormTopology", conf, builder.createTopology());

//          let the topology run for ____ seconds. Only for testing!
            Utils.sleep(30000);

//          kill the topology
            cluster.killTopology("StormTopology");

//          shutdown local cluster
            cluster.shutdown();

        }

    }
}
