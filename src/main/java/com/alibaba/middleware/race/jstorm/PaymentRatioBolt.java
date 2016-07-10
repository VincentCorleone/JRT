package com.alibaba.middleware.race.jstorm;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentAmountBin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class PaymentRatioBolt implements IRichBolt {


    protected OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(PaymentRatioBolt.class);

    HashMap<String, PaymentAmountBin> hashmap;
    protected int minMinutestamp ;
    protected int maxMinutestamp ;


    protected int rangeSizeOnInputTair;
    protected int offset;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        new TairOperatorImpl();
        hashmap = new HashMap<String, PaymentAmountBin>();
        minMinutestamp = 0;
        maxMinutestamp = 0;
        //必须是60的倍数
        rangeSizeOnInputTair = 3600;
        offset = 600;
    }

    @Override
    public void execute(Tuple tuple) {

        if (tuple.getSourceStreamId().equals(RaceConfig.MqPayTopic)) {

            int minutestamp = (int) tuple.getValueByField(RaceConfig.Minutestamp);
            double paymentAmount = (double) tuple.getValueByField(RaceConfig.PaymentAmount);
            short platform = (short) tuple.getValueByField(RaceConfig.PaymentAmount);

            if (minutestamp == 0) {
                hashMapAllToTair();
            } else {
                setRange(minutestamp);
                String minutestampStr = String.valueOf(minutestamp);
                String resultString = putInHashMap(minutestampStr, platform, paymentAmount);
                LOG.info(resultString);
            }
            collector.ack(tuple);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

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

    private String putInHashMap(String key, short payPlatform, double value) {
        PaymentAmountBin paymentAmountBin = hashmap.get(key);
        if (paymentAmountBin == null) {
            paymentAmountBin = new PaymentAmountBin();
            if (payPlatform == 0) {
                //PC
                paymentAmountBin.pcAmount = value;
                paymentAmountBin.wirelessAmount = 0.0;
            } else {
                //无线
                paymentAmountBin.pcAmount = 0.0;
                paymentAmountBin.wirelessAmount = value;
            }
            hashmap.put(key, paymentAmountBin);
        } else {
            if (payPlatform == 0) {
                //PC
                paymentAmountBin.pcAmount += value;
            } else {
                //无线
                paymentAmountBin.wirelessAmount += value;
            }
            hashmap.put(key, paymentAmountBin);
        }
        return "{" + key + "," + Double.toString(paymentAmountBin.pcAmount) + "," + Double.toString(paymentAmountBin.wirelessAmount) + "}";
    }

    private void setRange(int minutestamp) {
        if (minMinutestamp == 0) {
            minMinutestamp = minutestamp;
            maxMinutestamp = minutestamp;
        } else {
            if (minutestamp < minMinutestamp) {
                minMinutestamp = minutestamp;
            } else if (minutestamp > maxMinutestamp) {
                maxMinutestamp = minutestamp;
            }
        }

        if ((maxMinutestamp - minMinutestamp) > rangeSizeOnInputTair) {
            PaymentAmountBin paymentAmountBinSum = new PaymentAmountBin();
            paymentAmountBinSum.pcAmount = 0.0;
            paymentAmountBinSum.wirelessAmount = 0.0;
            for (int i = minMinutestamp; i < (maxMinutestamp - offset); i += 60) {

                String tmpkey = String.valueOf(i);
                PaymentAmountBin paymentAmountBinThisMinute = hashmap.get(tmpkey);
                paymentAmountBinSum.pcAmount += paymentAmountBinThisMinute.pcAmount;
                paymentAmountBinSum.wirelessAmount += paymentAmountBinThisMinute.wirelessAmount;

                double ratio = paymentAmountBinSum.pcAmount / paymentAmountBinSum.wirelessAmount;
                String finalkey = RaceConfig.prex_ratio + RaceConfig.teamcode + "_" + tmpkey;
                TairOperatorImpl.write(finalkey, new Double(ratio));
                hashmap.remove(tmpkey);
            }
            minMinutestamp = maxMinutestamp - offset;
        }
    }

    private void hashMapAllToTair() {
        Iterator it = hashmap.keySet().iterator();
        while (it.hasNext()) {
            String tmpkey = (String) it.next();
            PaymentAmountBin paymentAmountBin = hashmap.get(tmpkey);
            double ratio = paymentAmountBin.pcAmount / paymentAmountBin.wirelessAmount;
            String finalkey = RaceConfig.prex_ratio + RaceConfig.teamcode + "_" + tmpkey;
            TairOperatorImpl.tairManager.put(RaceConfig.TairNamespace, finalkey, new Double(ratio));
            System.out.println("[*] Write-into Tair.");

        }
        hashmap.clear();
    }
}
