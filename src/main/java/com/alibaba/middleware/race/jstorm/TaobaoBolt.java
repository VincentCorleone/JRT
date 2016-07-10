package com.alibaba.middleware.race.jstorm;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentAmountBin;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class TaobaoBolt implements IRichBolt {


    protected OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(TaobaoBolt.class);

    public static HashMap<Integer,Double> hashmap = new HashMap<Integer,Double>();
    public static HashMap<Integer,Double> tempmap = new HashMap<Integer,Double>();
    public static int MaxTimeStamp = 0;
    protected int minMinutestamp = 0;
    protected int maxMinutestamp = 0;

    //必须是60的倍数
    protected final int rangeSizeOnInputTair = 3600;
    protected final int offset = 600;

    private TairOperatorImpl tairOperator = new TairOperatorImpl();



    @Override
    public void execute(Tuple tuple) {
    	String topic = tuple.getSourceStreamId();
    	if(RaceConfig.MqTaobaoTradeTopic.equals(topic))
    	{
    		
    		try{
    			Integer Createtime   = (Integer) tuple.getValue(0);
    			Double  PaymentAmount = (Double) tuple.getValue(1);
    			Double value = TaobaoBolt.hashmap.get(Createtime);
    			if(value == null){
    				try{
    				TaobaoBolt.hashmap.put(Createtime, PaymentAmount);
    				if(TaobaoBolt.MaxTimeStamp < Createtime.intValue())
    					{
    						TaobaoBolt.MaxTimeStamp = Createtime.intValue();
    					}
    				}
    				catch(Exception e)
    				{
    					System.out.println("[*] Hash-Bolt write-in failed.");
    					e.printStackTrace();
    				}
    			}
    			else{
    				try{
    				TaobaoBolt.hashmap.put(Createtime,value+PaymentAmount);
    				}
    				catch(Exception e)
    				{
    					System.out.println("[*] Hash-Bolt write-in failed.");
    					e.printStackTrace();
    				}
    				if(TaobaoBolt.MaxTimeStamp < Createtime.intValue())
					{
						TaobaoBolt.MaxTimeStamp = Createtime.intValue();
					}
    			}
    			
    		}catch(Exception e){
    			System.out.println("[*] Bolt Get Value Failed.");
    			e.printStackTrace();
    		}
    		
    		if(TaobaoBolt.hashmap.size()>500){
    			//Write - into tair now.
    			Integer range = TaobaoBolt.MaxTimeStamp-10;  //10 MIN
    			Iterator it = TaobaoBolt.hashmap.keySet().iterator();
    			while(it.hasNext()){
    				Integer Key   = (Integer)it.next();   //Key
    				Double  Value = TaobaoBolt.hashmap.get(Key); //Value
    				if(Key.intValue()>range.intValue())
    				{
    					//Store it temprorialy.
    					TaobaoBolt.tempmap.put(Key, Value);
    				}
    				else{
    					//Write-in Tair.
    					int intkey     = Key.intValue();
    					String partkey = String.valueOf(intkey);
    					String keY = RaceConfig.prex_taobao + RaceConfig.teamcode + "_" + partkey;
    					tairOperator.write(keY, Value);
    				}
    				
    			}
    			TaobaoBolt.hashmap.clear();
    			Iterator temp = TaobaoBolt.tempmap.keySet().iterator();
    			while(temp.hasNext()){
    				Integer key  = (Integer)temp.next();
    				Double  value= TaobaoBolt.hashmap.get(key);
    				TaobaoBolt.hashmap.put(key, value);
    			}
    		}
    		
    		collector.ack(tuple);
    		
    		
    		
    	}
        
        
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