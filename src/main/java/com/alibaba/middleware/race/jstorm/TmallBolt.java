package com.alibaba.middleware.race.jstorm;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;


import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class TmallBolt implements IRichBolt {


    protected OutputCollector collector;
    private static Logger LOG = LoggerFactory.getLogger(TmallBolt.class);

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
    	if(RaceConfig.MqTmallTradeTopic.equals(topic))
    	{
    		
    		try{
    			Integer Createtime   = (Integer) tuple.getValue(0);
    			Double  PaymentAmount = (Double) tuple.getValue(1);
    			Double value = TmallBolt.hashmap.get(Createtime);
    			if(value == null){
    				try{
    				TmallBolt.hashmap.put(Createtime, PaymentAmount);
    				if(TmallBolt.MaxTimeStamp < Createtime.intValue())
    					{
    					TmallBolt.MaxTimeStamp = Createtime.intValue();
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
    					TmallBolt.hashmap.put(Createtime,new Double(value+PaymentAmount));
    				}
    				catch(Exception e)
    				{
    					System.out.println("[*] Hash-Bolt write-in failed.");
    					e.printStackTrace();
    				}
    				if(TmallBolt.MaxTimeStamp < Createtime.intValue())
					{
    					TmallBolt.MaxTimeStamp = Createtime.intValue();
					}
    			}
    			
    		}catch(Exception e){
    			System.out.println("[*] Bolt Get Value Failed.");
    			e.printStackTrace();
    		}
    		
    		if(TmallBolt.hashmap.size()>500){
    			//Write - into tair now.
    			Integer range = TmallBolt.MaxTimeStamp-10;  //10 MIN
    			Iterator it = TmallBolt.hashmap.keySet().iterator();
    			while(it.hasNext()){
    				Integer Key   = (Integer)it.next();   //Key
    				Double  Value = TmallBolt.hashmap.get(Key); //Value
    				if(Key.intValue()>range.intValue())
    				{
    					//Store it temprorialy.
    					TmallBolt.tempmap.put(Key, Value);
    				}
    				else{
    					//Write-in Tair.
    					int intkey     = Key.intValue();
    					String partkey = String.valueOf(intkey);
    					String keY = RaceConfig.prex_tmall + RaceConfig.teamcode + "_" + partkey;
    					TairOperatorImpl.tairManager.put(0,keY,Value);
    				}
    				
    			}
    			TmallBolt.hashmap.clear();
    			Iterator temp = TmallBolt.tempmap.keySet().iterator();
    			while(temp.hasNext()){
    				Integer key  = (Integer)temp.next();
    				Double  value= TmallBolt.hashmap.get(key);
    				TmallBolt.hashmap.put(key, value);
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
