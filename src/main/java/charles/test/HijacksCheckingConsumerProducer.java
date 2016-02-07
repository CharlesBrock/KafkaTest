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

public class HijacksCheckingConsumerProducer extends BaseKafkaOperation
{
    public static void main(String[] args) throws InterruptedException
    {
	HijacksHistory history = new HijacksHistory();
	
	HijacksHistoryCounterConsumer counter = new HijacksHistoryCounterConsumer(history);
	counter.startAllTopics();
	
	System.out.println("Waiting for all of the history to be read!");
	
	double lastTime = 0;
	double currentTime = 0;
	do
	{
	    Thread.sleep(1000);
	    lastTime = currentTime;
	    currentTime = history.getTotalTimeSum(); 
	}
	while(lastTime < currentTime);
	
	System.out.println("Starting to filter hijacks!");
    }

    @Override
    public String getProducerHost()
    {
	return localCluster;
    }

    @Override
    public String evaluateRecord(String record)
    {
	System.out.println(record);
	return null;
    }

    @Override
    public String getConsumerHost()
    {
	return localCluster;
    }

    @Override
    public boolean isValidTopic(String topic)
    {
	return topic.matches("HIJACKS.*");
    }

    @Override
    public String getDestinationTopic(String topic)
    {
	return "New-" + topic;
    }
}
