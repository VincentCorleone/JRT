package com.alibaba.middleware.race.rocketmq;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.log4j.Logger;

import java.util.Date;


public class MetaConsumerFactory {
	
	private static final Logger	LOG   = Logger.getLogger(MetaConsumerFactory.class);
	
    
    private static final long                 serialVersionUID = 4641537253577312163L;

	private static DefaultMQPushConsumer consumer = null;
    
    public static synchronized DefaultMQPushConsumer mkInstance(RocketMQClientConfig config,
			MessageListenerConcurrently listener)  throws Exception{
    	

    	if (consumer != null) {
    		
    		LOG.info("Consumer has been created, don't recreate it!");
    		//Attention, this place return null to info duplicated consumer
    		return null;
    	}
    	
        
        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init meta client \n");
        sb.append(",configuration:").append(config);
        
        LOG.info(sb.toString());
        
        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        
        String nameServer = config.getNameServer();
        if ( nameServer != null) {
			String namekey = "rocketmq.namesrv.domain";

			String value = System.getProperty(namekey);
			// this is for alipay
			if (value == null) {

				System.setProperty(namekey, nameServer);
			} else if (value.equals(nameServer) == false) {
				throw new Exception(
						"Different nameserver address in the same worker "
								+ value + ":" + nameServer);

			}
		}
        
        String instanceName = RaceConfig.MetaConsumerGroup +"@" +	JStormUtils.process_pid();
		consumer.setInstanceName(instanceName);
		/**
		 * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
		 * 如果非第一次启动，那么按照上次消费的位置继续消费
		 */
		//form consumer
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

		consumer.subscribe(RaceConfig.MqPayTopic, "*");
		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");

		consumer.registerMessageListener(listener);
		
		consumer.setPullThresholdForQueue(config.getQueueSize());
		consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
		consumer.setPullBatchSize(config.getPullBatchSize());
		consumer.setPullInterval(config.getPullInterval());
		consumer.setConsumeThreadMin(config.getPullThreadNum());
		consumer.setConsumeThreadMax(config.getPullThreadNum());


		Date date = config.getStartTimeStamp() ;
		if ( date != null) {
			LOG.info("Begin to reset meta offset to " + date);
			try {
				MQHelper.resetOffsetByTimestamp(MessageModel.CLUSTERING,
					instanceName, config.getConsumerGroup(),
					config.getTopic(), date.getTime());
				LOG.info("Successfully reset meta offset to " + date);
			}catch(Exception e) {
				LOG.error("Failed to reset meta offset to " + date);
			}

		}else {
			LOG.info("Don't reset meta offset  ");
		}

		consumer.start();
		

		LOG.info("Successfully create " + instanceName + " consumer");
		
		
		return consumer;
		
    }
    
}
