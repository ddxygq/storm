package com.demo;

/**
 * Created by Administrator on 2018/2/26.
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * 单词统计拓扑
 * @author zhengcy
 *
 */
public class WordCountTopology {

    public static void main(String[] args) throws Exception {

        // 建立topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SentenceSpout(), 1); // parallelism_hint 参数代表 executor 的数量
        builder.setBolt("split", new SplitSentenceBolt(), 2).shuffleGrouping("spout"); // shuffleGrouping：随意均匀地获取数据
        builder.setBolt("count", new WordCountBolt(), 2).fieldsGrouping("split", new Fields("word")); // fieldsGrouping，实际上是一个Hash，同一个词有相同hash，会被hash到同一个WordCount的Bolt里面去,计数

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            // 集群模式
            conf.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // 本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}