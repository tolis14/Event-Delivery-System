import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;


/*
 * This class is responsible for 
 * sending messages to other Brokers when 
 * changes in which they might be interested occurs.
 * It is used from Broker as a thread 1 per each
 * connection. Here the main functionality is to update other
 * Brokers about updates in his topic List, for example when a 
 * new topic is created and he is the responsible one for it.
 */
public class BrokerSpeaker implements Runnable{
	

	private ObjectOutputStream 			out;
	private volatile ArrayList<String> 	topicNames;
	private volatile ArrayList<Story>	storiesToUpdate;
	private int 						port;
	private int 						lastSeenTopic;
	private int 						lastSeenStory;
	public BrokerSpeaker(ObjectOutputStream out,ArrayList<String> topicNames, int port,ArrayList<Story> storiesToUpdate) {
		this.out 		= out;
		this.topicNames = topicNames;
		this.storiesToUpdate= storiesToUpdate;
		this.port 		= port;
		this.lastSeenTopic 	= 0;
		this.lastSeenStory  = 0;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(!Thread.currentThread().isInterrupted()) {
			try {
				/*
				 * Busy waiting. If no changes have occured in the topics or stories there is no need
				 * to notify other brokers. However when a new topic or a story is added to us we 
				 * should notify the other Brokers for such an update. Hence we notify them with these messages.
				 */
				while(!(this.lastSeenTopic == this.topicNames.size() || this.lastSeenStory == this.storiesToUpdate.size())) {}
				
				if(this.lastSeenTopic != this.topicNames.size()) {
					String lastTopic = this.topicNames.get(lastSeenTopic);
					this.lastSeenTopic++;
					Message m = new Message("Notify new topic","", lastTopic,String.valueOf(this.port));
					this.out.writeObject(m);
				}
				if(this.lastSeenStory != this.storiesToUpdate.size()) {
					System.out.println("speaker sent it");
					Story lastStory = this.storiesToUpdate.get(lastSeenStory);
					this.lastSeenStory++;
					Message m = new Message("Notify new story",lastStory);
					this.out.writeObject(m);
				}
			} 
			catch (IOException e) {
				System.out.println("Server disconnecting...");
				break;
			}
		}
	}

}
