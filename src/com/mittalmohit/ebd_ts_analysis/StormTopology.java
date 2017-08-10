/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis;

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

    public static final String HDFS_STREAM = "hdfs-stream";

    public StormTopology(String configFile) throws Exception {

    }

    public static void main(String[] args) throws Exception {
//      Create storm topology
        TopologyBuilder builder = new TopologyBuilder();

//        Create tweet spout
        String customerKey, secretKey, accessToken, accessSecret;
        customerKey = "7kvHWZGwJhVEop0LCkZ4mNykL";
        secretKey = "qB4k10J80TdBkiIuDcorkY1PkURDyvQAlE9PxHLPXcH1ksTTF3";
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

//        Build Topology
        builder.setSpout("tweet-spout", tweetSpout, 1);

//        builder.setBolt("parse-tweet-bolt", parseTweetBolt, 10).shuffleGrouping("tweet-spout");
//
//        builder.setBolt("word-count-bolt", wordCountBolt, 15).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));
//        builder.setBolt("final-bolt", finalBolt, 1).globalGrouping("word-count-bolt");
        builder.setBolt("sink-type-bolt", sinkTypeBolt, 1).shuffleGrouping("tweet-spout");
        builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("sink-type-bolt", HDFS_STREAM);

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
