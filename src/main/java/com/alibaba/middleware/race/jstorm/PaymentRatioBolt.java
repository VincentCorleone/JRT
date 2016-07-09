package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

public class PaymentRatioBolt implements IRichBolt {


    protected OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(PaymentRatioBolt.class);
    HashMap<String,List<Double>> hashmap = new HashMap<String,List<Double>>();

    @Override
    public void execute(Tuple tuple) {
        MetaTuple<PaymentMessage> metaTuple = (MetaTuple<PaymentMessage>)tuple.getValue(0);
        try {
            for(PaymentMessage paymentMessage : metaTuple.msgList){
                int Id =(int)paymentMessage.getCreateTime()/1000;
                int minutestamp = Id/60*60;
                String key = String.valueOf(minutestamp);
                Double value = null;
                Double PcValue=0.0;
                Double WirelessValue=0.0;
                short source = paymentMessage.getPaySource();
                if(source==0) //PC
                {
                     PcValue = paymentMessage.getPayAmount();
                }
                if(source==1) //Wireless
                {
                    WirelessValue = paymentMessage.getPayAmount();
                }

                if(hashmap.get(key)==null)
                {
                    hashmap.set(key,paymentMessage.getPayAmount());
                }
                hashmap.set(key,)
            }

            LOG.info("I am Jstorm , hello to Messages:" + metaTuple);

        } catch (Exception e) {
            collector.fail(tuple);
            return ;
            //throw new FailedException(e);
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}