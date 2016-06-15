package com.ks.demo;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class SendMessage {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties props = new Properties();
		props.put("zookeeper.connect", "10.133.47.102:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		props.put("metadata.broker.list", "10.133.47.102:6667");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		Random r = new Random();
		for(int i=0;i<100;i++){
			int id = r.nextInt(10000000);
			int memberid = r.nextInt(100000);
			int totalprice = r.nextInt(1000)+100;
			int youhui = r.nextInt(100);
			int sendpay = r.nextInt(3);
			
			StringBuffer data = new StringBuffer();
			data.append(String.valueOf(id))
			.append("\t")
			.append(String.valueOf(memberid))
			.append("\t")
			.append(String.valueOf(totalprice))
			.append("\t")
			.append(String.valueOf(youhui))
			.append("\t")
			.append(String.valueOf(sendpay))
			.append("\t")
			.append("2016-06-13");
			System.out.println(data.toString());
			producer.send(new KeyedMessage<String, String>("order",data.toString()));
		}
		producer.close();
		System.out.println("send over ------------------");
	}

}
