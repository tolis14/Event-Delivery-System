import java.util.ArrayList;

/*
 * This class is holds for every topic
 * the messages that have been created for
 * it. It is used in Brokers data structures.
 */
public class Topic {
	
	private ArrayList<Message> messages = null;
	private String name;
	
	public Topic(String name) {
		this.name = name;
		this.messages = new ArrayList<Message>();
		
	}
	
	String getName() {
		return this.name;
	}
	
	ArrayList<Message> getMessageList(){
		return this.messages;
	}
	
	void diplayMessages() {
		for (Message m : messages)
			System.out.println(m);
	}
	
	public String toString() {
		return "topic "+this.name;
	}
}
