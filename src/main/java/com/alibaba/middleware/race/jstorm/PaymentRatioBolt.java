package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentAmountBin;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PaymentRatioBolt implements IRichBolt {


    protected OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(PaymentRatioBolt.class);

    HashMap<String,PaymentAmountBin> hashmap = new HashMap<String,PaymentAmountBin>();
    protected int minMinutestamp = 0;
    protected int maxMinutestamp = 0;

    //必须是60的倍数
    protected final int rangeSizeOnInputTair = 3600;
    protected final int offset = 600;

    private TairOperatorImpl tairOperator = new TairOperatorImpl();



    @Override
    public void execute(Tuple tuple) {
        MetaTuple<PaymentMessage> metaTuple = (MetaTuple<PaymentMessage>)tuple.getValue(0);
        try {
            for(PaymentMessage paymentMessage : metaTuple.msgList){
                if(paymentMessage.getCreateTime()==0){
                    hashMapAllToTair();
                    break;
                }
                int minutestamp =((int)(paymentMessage.getCreateTime()/60000))*60;
                setRange(minutestamp);
                String minutestampStr = String.valueOf(minutestamp);
                String resultString = putInHashMap(minutestampStr,paymentMessage.getPayPlatform(),paymentMessage.getPayAmount());
                LOG.info(resultString);
            }

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

    private String putInHashMap(String key,short payPlatform,double value){
        PaymentAmountBin paymentAmountBin = hashmap.get(key);
        if(paymentAmountBin == null)
        {
            paymentAmountBin = new PaymentAmountBin();
            if(payPlatform == 0)
            {
                //PC
                paymentAmountBin.pcAmount = value;
                paymentAmountBin.wirelessAmount = 0.0;
            }else{
                //无线
                paymentAmountBin.pcAmount = 0.0;
                paymentAmountBin.wirelessAmount = value;
            }
            hashmap.put(key, paymentAmountBin);
        }else{
            if(payPlatform == 0)
            {
                //PC
                paymentAmountBin.pcAmount += value;
            }else{
                //无线
                paymentAmountBin.wirelessAmount += value;
            }
            hashmap.put(key, paymentAmountBin);
        }
        return  "{" + key + "," + Double.toString(paymentAmountBin.pcAmount) + "," + Double.toString(paymentAmountBin.wirelessAmount) + "}";
    }

    private void setRange(int minutestamp){
        if( minMinutestamp == 0 ){
            minMinutestamp = minutestamp;
            maxMinutestamp = minutestamp;
        }else{
            if(minutestamp < minMinutestamp){
                minMinutestamp = minutestamp;
            }else if(minutestamp > maxMinutestamp){
                maxMinutestamp = minutestamp;
            }
        }

        if ((maxMinutestamp - minMinutestamp)>rangeSizeOnInputTair){
            for (int i=minMinutestamp;i < (maxMinutestamp-offset);i+=60){
                String tmpkey = String.valueOf(i);
                PaymentAmountBin paymentAmountBin= hashmap.get(tmpkey);
                double ratio = paymentAmountBin.pcAmount/paymentAmountBin.wirelessAmount;
                tairOperator.write(tmpkey,ratio);
                hashmap.remove(tmpkey);
            }
            minMinutestamp = maxMinutestamp - offset;
        }
    }

    private void hashMapAllToTair(){
        for (int i=minMinutestamp;i <= maxMinutestamp;i+=60){
            String tmpkey = String.valueOf(i);
            PaymentAmountBin paymentAmountBin= hashmap.get(tmpkey);
            double ratio = paymentAmountBin.pcAmount/paymentAmountBin.wirelessAmount;
            tairOperator.write(tmpkey,ratio);
            hashmap.remove(tmpkey);
        }
    }
}