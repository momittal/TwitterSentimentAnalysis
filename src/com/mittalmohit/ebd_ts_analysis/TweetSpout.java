/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mittalmohit.ebd_ts_analysis;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.FilterQuery;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;

/**
 *
 * @author mohit
 */
public class TweetSpout extends BaseRichSpout {

//    Twitter Credentials
    String customerKey, secretKey, accessToken, accessSecret;

//    Create spout output collector to collect tuples from spout to next stage bolt
    SpoutOutputCollector collector;

    TwitterStream twitterStream;

//    Shared Queue for getting buffering received tweets
    LinkedBlockingQueue<String> queue = null;

//    Class to listen to twitter streaming api
    public TweetSpout(String key, String secret, String token, String aSecret) {
        customerKey = key;
        secretKey = secret;
        accessToken = token;
        accessSecret = aSecret;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
//            create buffer to block tweets
        queue = new LinkedBlockingQueue<String>(1000);

//          save output collector for emitting tuples
        collector = spoutOutputCollector;

//        build configuration with twitter credentials
        ConfigurationBuilder config = new ConfigurationBuilder()
                .setOAuthConsumerKey(customerKey)
                .setOAuthConsumerSecret(secretKey)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessSecret);

        twitterStream = new TwitterStreamFactory(config.build()).getInstance();
        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {
                queue.offer(status.getText());
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

//    provide handler for twitter Stream
        twitterStream.addListener(listener);

        // Filter keywords
        String[] keyWords = {"Trump"};
        FilterQuery query = new FilterQuery().track(keyWords);
        twitterStream.filter(query);

    }

    @Override
    public void nextTuple() {
//        pick a tweet from buffer
        String twt = queue.poll();

//          if no tweet is available, wait for 50 ms and return
        if (twt == null) {
            Utils.sleep(50);
            return;
        }

//        Emit the tweet to next stage/bolt
        collector.emit(new Values(twt));

    }

    @Override
    public void close() {
        // shutdown the stream - when we exit
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // create the component config 
        Config conf = new Config();

        // set the parallelism for this spout to be 1
        conf.setMaxTaskParallelism(1);

        return conf;
    }

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }
}
