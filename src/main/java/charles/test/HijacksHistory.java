package charles.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class HijacksHistory
{
    final double threshold = .01;
    final int prefixThreshold = 8;
    
    private Map<Prefix, Double> totalTimeMap = new HashMap<>();
    
    // map of prefix to map of as to time at AS
    private Map<Prefix, Map<String, Long>> history = new HashMap<>();
    
    // map of thread to map of prefix to last announcement time
    private Map<Thread, Map<Prefix, Long>> lastAnnouncementByThread = new HashMap<>();
    
    synchronized boolean isAnnouncementGood(Prefix prefix, String AS, long time)
    {
	if(prefix.prefix <= prefixThreshold)
	    return true; // we don't care about the defaultish routes
	
	Map<Prefix, Long> lastAnnouncement = lastAnnouncementByThread.get(Thread.currentThread());
	if(lastAnnouncement == null)
	{
	    lastAnnouncement = new HashMap<>();
	    lastAnnouncementByThread.put(Thread.currentThread(), lastAnnouncement);
	}
	
	Long lastTimeSeen = lastAnnouncement.get(prefix);
	
	if(lastTimeSeen == null)
	{
	    int steps = 32;
	    for(Entry<Prefix, Long> entry : lastAnnouncement.entrySet())
	    {
		if(entry.getKey().isSubset(prefix) && entry.getKey().prefix - prefix.prefix < steps)
		{
		    steps = entry.getKey().prefix - prefix.prefix;
		    lastTimeSeen = entry.getValue();
		}
	    }
	}
	lastAnnouncement.put(prefix, time);
	if(lastTimeSeen == null)
	    return true;
	time -= lastTimeSeen;
	
	if(!history.containsKey(prefix))
	{
	    history.put(prefix, new HashMap<String, Long>());
	}
	Long timeAtAS = history.get(prefix).get(AS);
	if(timeAtAS == null)
	    timeAtAS = (long) 0;
	timeAtAS += time;
	history.get(prefix).put(AS, timeAtAS);
	
	double totalTime = 0;
	for(Entry<Prefix, Double> entry : totalTimeMap.entrySet())
	{
	    if(entry.getKey().isSubset(prefix))
	    {
		totalTime = Math.max(totalTime, entry.getValue());
	    }
	}
	
	return timeAtAS / totalTime > threshold;
    }
    
    synchronized public double getTotalTimeSum()
    {
	double totalTime = 0;
	for(Entry<Prefix, Double> entry : totalTimeMap.entrySet())
	{
	    totalTime += entry.getValue();
	}
	return totalTime;
    }
}
