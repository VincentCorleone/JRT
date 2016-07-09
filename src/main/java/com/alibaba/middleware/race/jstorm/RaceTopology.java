package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.rocketmq.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Config conf = new Config();
    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {
        //conf只存放jstorm的配置和metaQ消费组配置项
        conf.put("topology.writer.parallel",1);
        conf.put("topology.spout.parallel",1);
        conf.put("topology.name",RaceConfig.JstormTopologyName);
        conf.put("storm.cluster.mode","local");

        conf.put("meta.consumer.group",RaceConfig.MetaConsumerGroup);
//		conf.put("meta.nameserver","116.56.129.194:9876");

        //本地模式：启动生产者
        new Producer().beiginProduce();

        TopologyBuilder builder = setupBuilder();

        submitTopology(builder);
    }

    private static TopologyBuilder setupBuilder() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        int writerParallel = JStormUtils.parseInt(
                conf.get("topology.writer.parallel"), 1);

        int spoutParallel = JStormUtils.parseInt(
                conf.get("topology.spout.parallel"), 1);

        builder.setSpout("RaceSpout", new RaceSpout(), spoutParallel);

        builder.setBolt("PaymentRatioBolt", new PaymentRatioBolt(), writerParallel)
                .shuffleGrouping("RaceSpout");

        return builder;
    }

    private static void submitTopology(TopologyBuilder builder) {
        try {
            if (local_mode(conf)) {

                LocalCluster cluster = new LocalCluster();

                cluster.submitTopology(
                        String.valueOf(conf.get("topology.name")), conf,
                        builder.createTopology());

                Thread.sleep(200000);

                cluster.shutdown();
            } else {
                StormSubmitter.submitTopology(
                        String.valueOf(conf.get("topology.name")), conf,
                        builder.createTopology());
            }

        } catch (Exception e) {
            LOG.error(e.getMessage(), e.getCause());
        }
    }

    public static boolean local_mode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if (mode.equals("local")) {
                return true;
            }
        }

        return false;

    }
}