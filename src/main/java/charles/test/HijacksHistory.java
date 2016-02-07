package charles.test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

// This class would be much better if it kept the state of the internet and an assumed view of it from each AS
public class HijacksHistory
{
    final double threshold = .01;
    final int prefixThreshold = 8;
    final double suspiciousAsThreshold = .25;

    private Map<Prefix, Double> totalTimeMap = new HashMap<>();

    // map of prefix to map of as to time at AS
    private Map<Prefix, Map<String, Long>> prefixHostHistory = new HashMap<>();

    // map of thread to map of prefix to last announcement time
    private Map<Thread, Map<Prefix, Long>> lastAnnouncementByThread = new HashMap<>();

    // a map of as to announced prefixes
    private Map<String, Set<Prefix>> asGuestHistory = new HashMap<>();

    
    private double maxObservedAsScore = 0;
    
    long earliestTime = Long.MAX_VALUE;
    
    synchronized public boolean isSuspiciousAS(String AS)
    {
	Set<Prefix> prefixes = asGuestHistory.get(AS);

	if (prefixes == null || prefixes.size() < 2)
	    return false; // trust the little guys... as long as they are little

	double conflictingAsScore = 0;

	for (Prefix prefix : prefixes)
	{
	    Map<String, Long> prefixHosts = prefixHostHistory.get(prefix);

	    if (prefixHosts == null)
	    {
		int steps = 32;
		for (Entry<Prefix, Map<String, Long>> entry : prefixHostHistory.entrySet())
		{
		    if (entry.getKey().isSubset(prefix) && entry.getKey().prefix - prefix.prefix < steps)
		    {
			steps = entry.getKey().prefix - prefix.prefix;
			prefixHosts = entry.getValue();
		    }
		}
	    }
	    
	    if(prefixHosts == null)
		continue; // why do I need this?

	    conflictingAsScore += 1 - (1.0 / prefixHosts.size());
	}

	conflictingAsScore /= prefixes.size();

	if(conflictingAsScore > maxObservedAsScore)
	    maxObservedAsScore = conflictingAsScore;
	if(conflictingAsScore > .9 * maxObservedAsScore)
	    System.out.println(AS + " got a suspicion score of " + conflictingAsScore + " (Max Score So Far: " + maxObservedAsScore + ")");
	
	return conflictingAsScore > suspiciousAsThreshold;
    }

    synchronized boolean isAnnouncementGood(Prefix prefix, String AS, long time)
    {
	if(time < earliestTime)
	{
	    earliestTime = time;
	    System.out.println("New Earliest Time: " + time);
	}
	
	if (prefix.prefix <= prefixThreshold)
	    return true; // we don't care about the defaultish routes

	if (!asGuestHistory.containsKey(AS))
	    asGuestHistory.put(AS, new HashSet<Prefix>());
	asGuestHistory.get(AS).add(prefix);

	Map<Prefix, Long> lastAnnouncement = lastAnnouncementByThread.get(Thread.currentThread());
	if (lastAnnouncement == null)
	{
	    lastAnnouncement = new HashMap<>();
	    lastAnnouncementByThread.put(Thread.currentThread(), lastAnnouncement);
	}

	Long lastTimeSeen = lastAnnouncement.get(prefix);

	if (lastTimeSeen == null)
	{
	    int steps = 32;
	    for (Entry<Prefix, Long> entry : lastAnnouncement.entrySet())
	    {
		if (entry.getKey().isSubset(prefix) && entry.getKey().prefix - prefix.prefix < steps)
		{
		    steps = entry.getKey().prefix - prefix.prefix;
		    lastTimeSeen = entry.getValue();
		}
	    }
	}
	lastAnnouncement.put(prefix, time);
	if (lastTimeSeen == null)
	    return true;
	time -= lastTimeSeen;

	if (!prefixHostHistory.containsKey(prefix))
	{
	    prefixHostHistory.put(prefix, new HashMap<String, Long>());
	}
	Long timeAtAS = prefixHostHistory.get(prefix).get(AS);
	if (timeAtAS == null)
	    timeAtAS = (long) 0;
	timeAtAS += time;
	prefixHostHistory.get(prefix).put(AS, timeAtAS);

	double totalTime = 0;
	for (Entry<Prefix, Double> entry : totalTimeMap.entrySet())
	{
	    if (entry.getKey().isSubset(prefix))
	    {
		totalTime = Math.max(totalTime, entry.getValue());
	    }
	}

	return timeAtAS / totalTime > threshold;
    }

    synchronized public double getTotalTimeSum()
    {
	double totalTime = 0;
	for (Entry<Prefix, Double> entry : totalTimeMap.entrySet())
	{
	    totalTime += entry.getValue();
	}
	return totalTime;
    }
}
