package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String teamcode = "41180ejoix";
    public static String JstormTopologyName = "41180ejoix";
    public static String MetaConsumerGroup = "41180ejoix";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";

    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSlaveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 35431;

    //本地模式
//    public static String TairConfigServer = "116.56.129.194:5198";
//    public static String TairSlaveConfigServer = "";
//    public static String TairGroup = "group_1";
//    public static Integer TairNamespace = 0;


    //这些是Fields的名称
    public static String Minutestamp = "Minutestamp";
    public static String PaymentAmount = "PaymentAmount";
    public static String Platform = "Platform";
    //这些是Spout和Bolt的名称
    public static String RaceSpout = "Minutestamp";
    public static String PaymentRatioBolt = "PaymentRatio";
    public static String TaobaoBolt = "TaobaoBolt";
    public static String TmallBolt = "TmallBolt";

    public static boolean LocalMode = false;

}
