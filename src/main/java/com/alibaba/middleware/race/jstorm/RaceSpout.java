package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.MetaConsumerFactory;
import com.alibaba.middleware.race.rocketmq.RocketMQClientConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingDeque;

public class RaceSpout implements IRichSpout ,MessageListenerConcurrently{
    private static Logger LOG = LoggerFactory.getLogger(RaceSpout.class);


    protected SpoutOutputCollector collector;



    protected String id;
    protected boolean flowControl;
    protected boolean autoAck;

    protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;

    protected transient MetricClient metricClient;
    protected transient AsmHistogram waithHistogram;
    protected transient AsmHistogram processHistogram;

    protected transient DefaultMQPushConsumer consumer;


    long sendingCount;
    long startTime;
    boolean isStatEnable;
    int sendNumPerNexttuple;

    public RaceSpout(){

    }

    public void initMetricClient(TopologyContext context) {
        metricClient = new MetricClient(context);
        waithHistogram = metricClient.registerHistogram("MetaTupleWait", null);
        processHistogram = metricClient.registerHistogram("MetaTupleProcess",
                null);
    }




    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
        this.sendingQueue = new LinkedBlockingDeque<MetaTuple>();

        this.flowControl = JStormUtils.parseBoolean(
                conf.get(RocketMQClientConfig.META_SPOUT_FLOW_CONTROL), true);
        this.autoAck = JStormUtils.parseBoolean(
                conf.get(RocketMQClientConfig.META_SPOUT_AUTO_ACK), true);

        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init MetaSpout:").append(id);
        sb.append(", flowControl:").append(flowControl);
        sb.append(", autoAck:").append(autoAck);
        LOG.info( sb.toString());

        initMetricClient(context);

        RocketMQClientConfig clientConfig = RocketMQClientConfig.mkInstance(conf);

        try {
            consumer = MetaConsumerFactory.mkInstance(clientConfig, this);
        } catch (Exception e) {
            LOG.error("Failed to create Meta Consumer ", e);
            throw new RuntimeException("Failed to create MetaConsumer" + id, e);
        }

        if (consumer == null) {
            LOG.warn(id
                    + " already exist consumer in current worker, don't need to create again! ");

        }



        sendingCount = 0;
        startTime = System.currentTimeMillis();
        sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
        isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);

        LOG.info("Successfully init " + id);
    }

    @Override
    public void nextTuple() {
//        orgin editon
//        int n = sendNumPerNexttuple;
//        while (--n >= 0) {
//            Utils.sleep(10);
//            String sentence = CHOICES[_rand.nextInt(CHOICES.length)];
//            _collector.emit(new Values(sentence));
//        }
//        updateSendTps();

        MetaTuple metaTuple = null;
        try {
            metaTuple = sendingQueue.take();
        } catch (InterruptedException e) {
        }

        if (metaTuple == null) {
            return;
        }

        sendTuple(metaTuple);
    }

    @Override
    public void ack(Object id) {
        LOG.warn("Shouldn't go this function(function ack)");
    }

    @Override
    public void fail(Object id) {
        LOG.warn("Shouldn't go this function(function fail)");
//        _collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MetaTuple"));
    }

    private void updateSendTps() {
        if (!isStatEnable)
            return;

        sendingCount++;
        long now = System.currentTimeMillis();
        long interval = now - startTime;
        if (interval > 60 * 1000) {
            LOG.info("Sending tps of last one minute is " + (sendingCount * sendNumPerNexttuple * 1000) / interval);
            startTime = now;
            sendingCount = 0;
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.shutdown();
        }

    }

    @Override
    public void activate() {
        if (consumer != null) {
            consumer.resume();
        }

    }

    @Override
    public void deactivate() {
        if (consumer != null) {
            consumer.suspend();
        }
    }



    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    public void sendTuple(MetaTuple metaTuple) {
        metaTuple.updateEmitMs();
        collector.emit(new Values(metaTuple), metaTuple.getCreateMs());
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {

        List<PaymentMessage> PaymentMessages = new ArrayList<PaymentMessage>();
//    from consumer.java
        for (MessageExt msg : msgs) {

            byte [] body = msg.getBody();
            if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                //Info: 生产者停止生成数据, 并不意味着马上结束
                System.out.println("Got the end signal");
                PaymentMessage paymentMessage = new PaymentMessage();
                paymentMessage.setCreateTime(0);
                PaymentMessages.add(paymentMessage);
                continue;
            }

            PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
            PaymentMessages.add(paymentMessage);
            System.out.println(paymentMessage);
        }
        try {
            MetaTuple<PaymentMessage> metaTuple = new MetaTuple<PaymentMessage>(PaymentMessages, context.getMessageQueue());

            if (flowControl) {
                sendingQueue.offer(metaTuple);
            } else {
                sendTuple(metaTuple);
            }

            if (autoAck) {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            } else {
                metaTuple.waitFinish();
                if (metaTuple.isSuccess() == true) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } else {
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

        } catch (Exception e) {
            LOG.error("Failed to emit " + id, e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

    }
}