package charles.test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HijacksHistoryCounterConsumer implements Runnable
{
    String topic;
    int partition;

    HijacksHistory history;

    public HijacksHistoryCounterConsumer(String topic, int partition, HijacksHistory history)
    {
	this.topic = topic;
	this.partition = partition;
	this.history = history;
    }

    public static KafkaConsumer<String, String> getConsumer()
    {
	Properties consumerProperties = new Properties();
	consumerProperties.put("bootstrap.servers", "node1.kafka.ris.ripe.net:9092");
	consumerProperties.put("group.id", "cb-kafka-hijacks-history-counter");
	consumerProperties.put("enable.auto.commit", "false");
	consumerProperties.put("auto.commit.interval.ms", "1000");
	consumerProperties.put("session.timeout.ms", "30000");
	consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
	return consumer;
    }

    public void run()
    {
	KafkaConsumer<String, String> consumer = getConsumer();

	TopicPartition topicPartition = new TopicPartition(topic, partition);

	consumer.assign(Arrays.asList(topicPartition));
	consumer.seekToBeginning(topicPartition);

	while (true)
	{
	    ConsumerRecords<String, String> records = consumer.poll(100);
	    for (ConsumerRecord<String, String> record : records)
	    {
		try
		{
		    JsonElement element = new JsonParser().parse(record.value());
		    
		    JsonObject bgpMessage = element.getAsJsonObject();
		    
		    Double time = bgpMessage.get("time").getAsDouble();
		    
		    if (!bgpMessage.has("neighbor"))
			continue;
		    JsonObject neighbors = bgpMessage.get("neighbor").getAsJsonObject();
		    
		    JsonObject asn = neighbors.get("asn").getAsJsonObject();
		    String AS = asn.get("peer").getAsString();
		    
		    if (!neighbors.has("message"))
			continue;
		    JsonObject message = neighbors.get("message").getAsJsonObject();
		    
		    if (!message.has("update"))
			continue;
		    JsonObject update = message.get("update").getAsJsonObject();
		    
		    if (!update.has("announce"))
			continue;
		    JsonObject announce = update.get("announce").getAsJsonObject();
		    
		    for (Entry<String, JsonElement> announceEntry : announce.entrySet())
		    {
			if (announceEntry.getKey().contains("unicast"))
			{
			    continue;
			}
			for (Entry<String, JsonElement> innerEntry : announceEntry.getValue().getAsJsonObject()
				.entrySet())
			{
			    for (Entry<String, JsonElement> prefixEntry : innerEntry.getValue().getAsJsonObject()
				    .entrySet())
			    {
				history.isAnnouncementGood(new Prefix(prefixEntry.getKey()), AS, time.longValue());
			    }
			}
		    }
		} catch (ClassCastException | NullPointerException | IllegalStateException e)
		{
		    System.out.println("Can't parse: " + record.value());
		    e.printStackTrace();
		}
	    }
	    consumer.committed(topicPartition);
	}
    }
}
