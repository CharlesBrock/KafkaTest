package charles.test;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class CopyConsumerProducer implements Runnable
{
    String topic;
    int partition;

    public CopyConsumerProducer(String topic, int partition)
    {
	this.topic = topic;
	this.partition = partition;
    }

    @SuppressWarnings("resource")
    public void run()
    {
	Properties consumerProperties = new Properties();
	consumerProperties.put("bootstrap.servers", "node1.kafka.ris.ripe.net:2181");
	consumerProperties.put("group.id", "test");
	consumerProperties.put("enable.auto.commit", "false");
	consumerProperties.put("auto.commit.interval.ms", "1000");
	consumerProperties.put("session.timeout.ms", "30000");
	consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

	consumer.assign(Arrays.asList(new TopicPartition(topic, partition)));

	Properties producerProperties = new Properties();
	producerProperties.put("bootstrap.servers", "comet-17-08.sdsc.edu:4242");
	producerProperties.put("acks", "all");
	producerProperties.put("retries", 0);
	producerProperties.put("batch.size", 16384);
	producerProperties.put("linger.ms", 1);
	producerProperties.put("buffer.memory", 33554432);
	producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	Producer<String, String> producer = new KafkaProducer<>(producerProperties);

	while (true)
	{
	    System.out.println("retrieving...");
	    ConsumerRecords<String, String> records = consumer.poll(1000);
	    for (ConsumerRecord<String, String> record : records)
	    {
		System.out.println("putting...");
		ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>("cb_" + topic, partition,
			record.key(), record.value());
		producer.send(newRecord);
	    }
	    System.out.println("incrementing...");
	    consumer.commitSync();
	}
    }

    public static void main(String[] args)
    {
	//for (String topic : Arrays.asList("rib-rrc18", "rib-rrc19", "rib-rrc20", "rib-rrc21"))
	//{
	//    for (int partition = 0; partition < 10; partition++)
		
	for (String topic : Arrays.asList("rib-rrc18"))
	{
	    for (int partition = 0; partition < 1; partition++)	
	    {
		CopyConsumerProducer copier = new CopyConsumerProducer(topic, partition);
		Thread thread = new Thread(copier);
		thread.start();
	    }
	}
    }
}
