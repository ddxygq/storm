package com.trident;

import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by Administrator on 2018/2/27.
 */
public class TridentWordCount {
    public static void main(String[] args) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("setence"),3,
                new Values("the cow jump over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat")
        );

        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCount = topology.newStream("spout",spout)
                .each(new Fields("setence"),new Split(),new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"))
                .parallelismHint(6);
    }
}
