package charles.test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HijacksCheckingConsumerProducer extends BaseKafkaOperation
{
    private HijacksHistory history;
    
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
	return "test1-" + topic;
    }
    
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
	
	HijacksCheckingConsumerProducer checker = new HijacksCheckingConsumerProducer(history);
	checker.startAllTopics();
    }

}
