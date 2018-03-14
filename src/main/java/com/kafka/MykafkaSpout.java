package com.kafka;

import kafka.common.AuthorizationException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

public class MykafkaSpout {

    public static void main(String[] args) throws AuthorizationException {
        // TODO Auto-generated method stub

        // kafka订阅的主题
        String topic = "kg";
        // 配置zookeeper地址
        ZkHosts zkHosts = new ZkHosts("192.168.112.212:2181");
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic,
                "",
                "MyTrack");
        List<String> zkServers = new ArrayList<String>();
        zkServers.add("192.168.112.212");
        spoutConfig.zkServers = zkServers;
        spoutConfig.zkPort = 2181;
        spoutConfig.socketTimeoutMs = 60 * 1000;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
        builder.setBolt("bolt1", new MyKafkaBolt(), 1).shuffleGrouping("spout");

        Config config = new Config();
        config.setDebug(false);

        if (args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (org.apache.storm.generated.AuthorizationException e) {
                e.printStackTrace();
            }

        } else {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("mytopology", config, builder.createTopology());
        }

    }

}
