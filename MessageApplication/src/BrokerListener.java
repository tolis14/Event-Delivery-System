import java.io.IOException;
import java.io.ObjectInputStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


/*
 * This class is responsible for 
 * receiving the messages from other 
 * Brokers in the background and update the 
 * Brokers structures appropriately. Here it's main 
 * job is to take the messages for changes in topics
 * and update proportionally the topics per each broker.
 * Also, this class is updating other Brokers for new stories that are being uploaded to 
 * this Broker. Every broker has to hold the stories list.
 */
public class BrokerListener implements Runnable{

	private ObjectInputStream 						in;
	private volatile HashMap<Integer,ArrayList<String>> topicsPerBroker;
	private final ConcurrentHashMap<String,ArrayList<Story>> stories;


	public BrokerListener(ObjectInputStream in,HashMap<Integer,ArrayList<String>> topicsPerBroker,ConcurrentHashMap<String,ArrayList<Story>> stories) {
		this.in 				= in;
		this.topicsPerBroker 	= topicsPerBroker;
		this.stories			= stories;
	}
	
	
	
	@Override
    public synchronized void run() {
        // TODO Auto-generated method stub
        while(!Thread.currentThread().isInterrupted()) {
            try {
                Message m = (Message)in.readObject();
                if(m.getHeader().equals("Notify new topic")) {
                    String topic = m.getData();
                    int brokerID = Util.hashTopic(topic);
                    if(!this.topicsPerBroker.containsKey(brokerID)) {
                        ArrayList<String> topicsForBroker = new ArrayList<String>();
                        topicsForBroker.add(topic);
                        synchronized(this.topicsPerBroker) {
                            this.topicsPerBroker.put(brokerID, topicsForBroker);
                        }
                    }
                    else {
                            synchronized(this.topicsPerBroker) {
                                this.topicsPerBroker.get(brokerID).add(topic);
                            }
                    }
                    System.out.println("update new  topic");
                }
                else if (m.getHeader().equals("Notify new story")) {
                	System.out.println("listener got it");
                	Story story = m.getStory();
                	String sender = story.getSender();
                	if(!this.stories.containsKey(sender)) {
                        ArrayList<Story> newStories = new ArrayList<Story>();
                        newStories.add(story);

                        this.stories.put(sender, newStories);  
                       
                    }
                    else {
                    	this.stories.get(sender).add(story);
                    }
                	
                }
               
            } 
            catch (ClassNotFoundException | IOException | NoSuchAlgorithmException e) {
                System.out.println("Server disconnecting...");
                break;
            }
        }
    }
}
