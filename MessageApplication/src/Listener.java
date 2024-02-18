
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

/*
 * This class is responsible for receiving all the traffic 
 * from the broker. Every connection with a broker that the 
 * consumer has is initializing a new thread listener. Messages received
 * there are then passed in the consumer from the blocking queue which helps with 
 * synchronization
 */

public class Listener implements Runnable {
	public final static int MAX_MESSAGES = 20;
	private ObjectInputStream in;
	private final ArrayBlockingQueue<Message> Qmsg;
	private final HashMap<String,LinkedList<Message>> lastMessages;
	private HashMap<Integer,ArrayList<Message>> messagesBuffer;
	private String profileName;
	private Date lastDate;
	
	public Listener(ObjectInputStream in,ArrayBlockingQueue<Message> Qmsg,HashMap<String,LinkedList<Message>> lastMessages,String profileName ) {
		this.in 				= in;
		this.Qmsg 				= Qmsg;
		this.lastMessages	 	= lastMessages;
		this.messagesBuffer 	= new HashMap<Integer,ArrayList<Message>>();
		this.profileName 		= profileName;
	}

	@Override
	public void run() {
		
		System.out.println("Listener running...");
		while(!Thread.currentThread().isInterrupted()) {
			try {
				Message msg = (Message)in.readObject();
				
				//message with MMfile or Story received.
				if(msg.getHeader().equals("MMFile")|| msg.getHeader().equals("Pick story reply")) {
					int id = msg.getId();
					if(this.messagesBuffer.containsKey(id)) {
						this.messagesBuffer.get(id).add(msg);
					}
					else {
						ArrayList<Message> msgs= new ArrayList<Message>();
						msgs.add(msg); //chuncks for the MMfile
						this.messagesBuffer.put(id,msgs);		
					}
					
					if(this.messagesBuffer.get(id).size()==msg.getNoChunks()) {
						Collections.sort(this.messagesBuffer.get(id));
						
						int size = (messagesBuffer.get(id).size() - 1) * Publisher.CHUNK_SIZE + msg.getBytesLength();
						byte[] mmBytes = new byte[size];
						int counter = 0;
						for(Message m : messagesBuffer.get(id)) {
							for(int i=0;i<m.getBytesLength();i++)
								mmBytes[i+counter] = m.getBytes()[i];
							counter += Publisher.CHUNK_SIZE;
						}
						MultimediaFile mmfile = new MultimediaFile(msg.getFileName(),msg.getSender(),mmBytes);
						Message m=null;
						if(msg.getHeader().equals("MMFile")) {
							m = new Message(msg.getHeader(), msg.getTopic(), mmfile, msg.getSender(), msg.getFileName());
						}
						else {
							Story story = new Story(mmfile,msg.getSender(),lastDate);
							m = new Message("Pick story reply",story);
						}
						
						/*
						 * Store the messages and stories multimedia file
						 * in the client's directory.
						 */
						String dirPath = "..\\ClientsDirectories\\" + this.profileName+"\\";
						String[] filesNameAndExtension = mmfile.getFileName().split("\\.");
						File file=new File(dirPath+filesNameAndExtension[0]+"."+filesNameAndExtension[1]);
                        FileOutputStream fos=new FileOutputStream(file);
                        fos.write(mmBytes);
                        fos.close();
						
						if(Qmsg.size()<20) 
							this.Qmsg.add(m);
						
						if(this.lastMessages.size()>0 && msg.getHeader().equals("MMFile")) {
							String key = msg.getTopic();
							if(this.lastMessages.get(key).size() >= MAX_MESSAGES) {
								this.lastMessages.get(key).pop();
							}
							this.lastMessages.get(key).add(msg);
						}
						this.messagesBuffer.get(id).clear();					
					}
					
				}
				//message with pure text received.
				else if(msg.getHeader().equals("Text")){
					if(Qmsg.size()<20) {
						this.Qmsg.add(msg);
					}
					if(this.lastMessages.size()>0 && msg.getHeader().equals("Text")) {
						String key = msg.getTopic();
						if(this.lastMessages.get(key).size() >= MAX_MESSAGES) {
							this.lastMessages.get(key).pop();
						}
						this.lastMessages.get(key).add(msg);
					}
				}
				else if(msg.getHeader().equals("Story Time")) {
					 lastDate = (Date)in.readObject(); //date for story that server is about to send us.
				}
				//every other protocol message. More control is implemented via consumer who takes messages from the queue.
				else{
					if(Qmsg.size()<20) {
						this.Qmsg.add(msg);
					}	
				}
			}
			catch (ClassNotFoundException | IOException e) {
				break;
			}
		}
	}
}
