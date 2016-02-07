package charles.test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import javax.xml.soap.Detail;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class HijacksCheckingConsumerProducer implements Runnable
{
    private static KafkaProducer<String, String> producer = null;

    String sourceTopic;
    String destinationTopic;
    int partition;

    public HijacksCheckingConsumerProducer(String sourceTopic, int partition, String destinationTopic)
    {
	this.sourceTopic = sourceTopic;
	this.partition = partition;
	this.destinationTopic = destinationTopic;
    }

    public static KafkaConsumer<String, String> getConsumer()
    {
	Properties consumerProperties = new Properties();
	consumerProperties.put("bootstrap.servers", "comet-17-08.sdsc.edu:9092");
	consumerProperties.put("enable.auto.commit", "false");
	consumerProperties.put("auto.commit.interval.ms", "1000");
	consumerProperties.put("session.timeout.ms", "30000");
	consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
	return consumer;
    }

    public static KafkaProducer<String, String> getProducer()
    {
	if (producer == null)
	{

	    Properties producerProperties = new Properties();
	    producerProperties.put("bootstrap.servers", "comet-17-08.sdsc.edu:9092");
	    producerProperties.put("acks", "all");
	    producerProperties.put("retries", 0);
	    producerProperties.put("batch.size", 16384);
	    producerProperties.put("linger.ms", 1);
	    producerProperties.put("buffer.memory", 33554432);
	    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

	    producer = new KafkaProducer<>(producerProperties);
	}
	return producer;
    }

    @SuppressWarnings({ "rawtypes" })
    public void run()
    {
	KafkaConsumer<String, String> consumer = getConsumer();

	TopicPartition topicPartition = new TopicPartition(sourceTopic, partition);

	consumer.assign(Arrays.asList(topicPartition));
	consumer.seekToBeginning(topicPartition);

	Producer<String, String> producer = getProducer();

	while (true)
	{
	    ConsumerRecords<String, String> records = consumer.poll(100);
	    List<Future> futures = new LinkedList<>();
	    for (ConsumerRecord<String, String> record : records)
	    {
		ProducerRecord<String, String> newRecord = new ProducerRecord<String, String>("cb_" + sourceTopic, 0,
			record.key(), record.value());
		futures.add(producer.send(newRecord));
	    }
	    while (futures.size() > 0)
	    {
		Iterator<Future> futureIterator = futures.iterator();
		while (futureIterator.hasNext())
		{
		    if (futureIterator.next().isDone())
			futureIterator.remove();
		}
	    }
	    consumer.committed(topicPartition);
	    if (records.count() == 0)
	    {
		try
		{
		    Thread.sleep(10000);
		} catch (InterruptedException e)
		{
		    throw new IllegalStateException(e);
		}
	    }
	}
    }

    public static void main(String[] args) throws InterruptedException
    {
	Map<String, List<PartitionInfo>> topics = HijacksHistoryCounterConsumer.getConsumer().listTopics();

	HijacksHistory history = new HijacksHistory();

	for (String topic : topics.keySet())
	{
	    if (topic.matches("raw-rrc.*"))
	    {
		for (PartitionInfo partitionInfo : topics.get(topic))
		{
		    HijacksHistoryCounterConsumer copier = new HijacksHistoryCounterConsumer(topic,
			    partitionInfo.partition(), history);
		    Thread thread = new Thread(copier);
		    thread.start();
		}
	    }
	}
	double lastTime = 0;
	double currentTime = 0;
	do
	{
	    Thread.sleep(1000);
	    lastTime = currentTime;
	    currentTime = history.getTotalTimeSum(); 
	}
	while(lastTime < currentTime);
    }
}
