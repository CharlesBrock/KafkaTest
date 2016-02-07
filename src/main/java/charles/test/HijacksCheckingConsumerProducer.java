package charles.test;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HijacksCheckingConsumerProducer extends BaseKafkaOperation
{
    private HijacksHistory history;
    
    static final String filteredConflictTopicPrefix = "test6-";
    static final String suspiciousAsTopic = "test1-suspicious-as";
    
    public HijacksCheckingConsumerProducer(HijacksHistory history)
    {
	this.history = history;
    }
    
    @Override
    public String getProducerHost()
    {
	return localCluster;
    }

    @Override
    public String evaluateRecord(String record)
    {
	JsonObject conflict = new JsonParser().parse(record).getAsJsonObject();
	String[] path = conflict.get("as_path").getAsString().split("\\s+");
	String AS = path[path.length - 1];
	
	Double time = conflict.get("timestamp").getAsDouble();
	
	String prefixString = conflict.get("prefix").getAsString();
	Prefix prefix = new Prefix(prefixString);
	
	if(history.isAnnouncementGood(prefix, AS, time.longValue()))
	{
	    System.out.println("   Seen before: " + record);
	    return null;
	}
	
	if(history.isSuspiciousAS(AS))
	{
	    ProducerRecord<String, String> kafkaRecord = new ProducerRecord<String, String>(suspiciousAsTopic, 0, "", AS);
	    getProducer().send(kafkaRecord);
	    System.out.println("Suspicious AS:destinationTopic " + AS);
	}
	
	System.out.println("New Record: " + record);
	return record;
    }

    @Override
    public String getConsumerHost()
    {
	return localCluster;
    }

    @Override
    public boolean isValidTopic(String topic)
    {
	return topic.matches("rib-rrc.*");
    }

    @Override
    public String getDestinationTopic(String topic)
    {
	return filteredConflictTopicPrefix + topic;
	//return "unseen-" + topic;
    }
    
    public static void main(String[] args) throws InterruptedException
    {
	HijacksHistory history = new HijacksHistory();
	
	HijacksHistoryCounterConsumer counter = new HijacksHistoryCounterConsumer(history);
	counter.startAllTopics();
	
	Thread.sleep(1000);
	
	System.out.println("Waiting for all of the history to be read!");
	
	double lastTime = 0;
	double currentTime = 0;
	do
	{
	    Thread.sleep(1000);
	    lastTime = currentTime;
	    currentTime = history.getTotalTimeSum();
	    System.out.println(lastTime + " -> " + currentTime + " processed " 
		    + HijacksHistoryCounterConsumer.processedMessages + " / " + HijacksHistoryCounterConsumer.intermediateMessages
		    + " / " + HijacksHistoryCounterConsumer.observedMessages);
	}
	while(HijacksHistoryCounterConsumer.observedMessages < 200000);
	
	System.out.println("Starting to filter hijacks!");
	
	HijacksCheckingConsumerProducer checker = new HijacksCheckingConsumerProducer(history);
	checker.startAllTopics();
    }

}
