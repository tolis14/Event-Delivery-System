import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/*
 * This class is responsible for handling 
 * consumers and publishers and also update
 * Brokers structures given the requests of the
 * clients. It also implements some protocols in order
 * to be able to decode the messages that he receives from
 * clients.
 */
public class ClientHandler implements Runnable{

	ObjectInputStream  in 	= null;
	ObjectOutputStream out 	= null;
	Socket connection		= null;
	
	//This is the map for the chunks. Integer is the id of the message and List is the chunks for this specific message.
	private HashMap<Integer,ArrayList<Message>> messagesBuffer; 
	
	private HashMap<String,Topic> topics;
	
	/*
	 * This is the map for the users that are subscribed to a topic. 
	 * String is the topicName and Integer is the number of last message
	 * that this subscriber has received for the specific topic
	 */
	private HashMap<String,Integer> subscriberTopics;
	
	
	private volatile ArrayList<String> topicNames; //name of topics that are being added dynamically. Used for synchronization with Brokers.
	private volatile HashMap<Integer,ArrayList<String>> topicsPerBroker; // HashMap that holds for every other broker the topics that he resp for.
	
	private final ConcurrentHashMap<String,ArrayList<Story>> stories; // stories per user.
	private volatile ArrayList<Story> storiesToUpdate; // new stories that has been added to us and should get transimmited to other Brokers.
	
	private int msgId;
	
	public ClientHandler(Socket connection, HashMap<String,Topic> topics, ArrayList<String> topicNames,
						 HashMap<Integer,ArrayList<String>> topicsPerBroker,ConcurrentHashMap<String,ArrayList<Story>> stories,
						 ArrayList<Story> storiesToUpdate) {
		
		this.connection 		= connection;
		this.messagesBuffer 	= new HashMap<Integer,ArrayList<Message>>();
		this.subscriberTopics 	= new HashMap<String,Integer>();
		this.topics 			= topics;
		this.topicNames 		= topicNames;
		this.topicsPerBroker 	= topicsPerBroker;
		this.stories 			= stories;
		this.storiesToUpdate	= storiesToUpdate;
		this.msgId				= 0;
		try {
			out 				= new ObjectOutputStream(connection.getOutputStream());
			in 					= new ObjectInputStream(connection.getInputStream());
		}
		catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * This method checks until which message our subscriber
	 * has received for every topic that is registered. If the subscriber
	 * is not updated we forwarding every new message for him.
	 */
	private void forwardToClient() throws IOException {
		
		for(Map.Entry<String, Integer> set: this.subscriberTopics.entrySet()) {
			String key = set.getKey();
			int value  = set.getValue();
			int size   = this.topics.get(key).getMessageList().size();
			if(size > value){
				for(int i = value;i<size;i++) {
					Message m = this.topics.get(key).getMessageList().get(i);
					if(m.getHeader().equals("Text")) {
						out.writeObject(m);
						out.flush();
					}
					else {
						ArrayList<byte[]> chunks = Util.generateChunks(m.getMMFile());
						this.msgId++;
						int chunkSeq = 0;
						for(byte[] chunk : chunks) {
							Message msg = new Message("MMFile", m.getTopic(),chunk,m.getSender(),m.getFileName(),this.msgId, chunks.size(),chunkSeq++);
							try {
								out.writeObject(msg);
								out.flush();
							} 
							catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
				this.subscriberTopics.replace(key, size); // update until which message this client has received for every of his topics.
			}
		}
	}
	
	
	/*
	 * This is the main method of this class. Its purpose is
	 * to process the messages received from publishers and subscribers
	 * and service their requests.
	 */
	@Override
	public synchronized void run() {
		
		while(true) {
			try {
				
				Message msg = (Message)in.readObject();
				
				String header = msg.getHeader();
				String messageTopic = msg.getTopic();
				String data = "";
				String sender = msg.getSender();
				
				if(!header.equals("MMFile")) {
					data = msg.getData();
				}
				
				//request for topic creation (Publisher).
				if(header.equals("Create topic")) {
					String topicName = data;
					if(!this.topics.containsKey(topicName)) {
						this.topics.put(topicName, new Topic(topicName));
						this.topicNames.add(topicName);
						Message m=new Message("Topic success", "" , "Topic successfully created", "");
						out.writeObject(m);
						out.flush();
						System.out.println("Topic created");
						
					}
					else {
						System.out.println("This topic already exists");
						Message m=new Message("Topic failed", "" , "Topic already exists", "");
						out.writeObject(m);
						out.flush();
					}
					continue;
				}
				//request for IP (Both).
				else if(header.equals("Probe IP")) {
					String topicName = data;
					String[] connectionID = Util.findIpPort(topicName);
					String replyData = connectionID[0]+" "+connectionID[1];
					Message reply = new Message("Probe IP Reply", topicName, replyData, sender);
					out.writeObject(reply);
					System.out.println("Reply for ip");
				}
				//request for disconnection (Both).
				else if(header.equals("Disconnection")) {
					break;
				}
				//request for subscription in topic
				else if(header.equals("Register")) {
					this.subscriberTopics.put(data, 0);
					continue;	
				}
				//request for topics list (Consumer).
				else if(header.equals("Get topics")) {
					String topicsList = "";
					for(Topic t: this.topics.values()) {
						topicsList += t.getName()+"\n";
					}
					for (ArrayList<String> topics: this.topicsPerBroker.values()) {
						for(String s:topics)
							topicsList += (s +"\n");
					}
					if(!topicsList.equals("")) {
						topicsList = topicsList.substring(0, topicsList.length() - 1);
					}
					Message reply = new Message("Topics List", null, topicsList, sender);
					out.writeObject(reply);
					out.flush();
					continue;
				}
				//request for full conversation messages (Consumer).
				else if(header.equals("Get info")) {
					byte[] bytes;
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					ObjectOutputStream oos;
					oos = new ObjectOutputStream(baos);
					oos.writeObject(this.topics.get(messageTopic).getMessageList());
					bytes = baos.toByteArray();
					Message m = new Message("Topic conversation",null,bytes,sender,"",0,0,0);
					out.writeObject(m);
					out.flush();
					continue;
				}
				//request for getting new messages in the topic (Consumer).
				else if(header.equals("Notify for new messages")) {
					this.forwardToClient();
					continue;
				}
				//receiving message with Multimedia file from client (Publisher).
				else if(header.equals("MMFile") || header.equals("Story")) {
					int id = msg.getId();
					String fileName = msg.getFileName();
					
					/*
					 * If the file is multimedia then we have to firstly 
					 * take all of his chunks and then after reconstructing the 
					 * message store them to topics structure.
					 */
					if(messagesBuffer.containsKey(id)) {
						messagesBuffer.get(id).add(msg);
						if(messagesBuffer.get(id).size() == msg.getNumOfChunks()) { //here we know that last chunk of the message has arrived.
							System.out.println("Multimedia message arrived");
							Collections.sort(messagesBuffer.get(id)); // sorts chunks for message arrived
							int size = (messagesBuffer.get(id).size() - 1) * Publisher.CHUNK_SIZE + msg.getBytesLength();
							byte[] mmBytes = new byte[size];
							int counter = 0;
							for(Message m : messagesBuffer.get(id)) {
								for(int i=0;i<m.getBytesLength();i++)
									mmBytes[i+counter] = m.getBytes()[i];
								counter += Publisher.CHUNK_SIZE;
							}
							MultimediaFile mmfile = new MultimediaFile(fileName,sender,mmBytes); //reconstructing the multimediafile.
							
							if(header.equals("MMFile")) {
								Message m = new Message(header, messageTopic, mmfile, sender, fileName);
								topics.get(messageTopic).getMessageList().add(m);
							}
							else if(header.equals("Story")) {
								Story story=new Story(mmfile,sender);
								if(this.stories.containsKey(sender)) {
									stories.get(sender).add(story);
								}
								else {
									ArrayList<Story> newstories = new ArrayList<Story>();
									newstories.add(story);
									stories.put(sender,newstories);
								}
								this.storiesToUpdate.add(story);
								System.out.println("Clienthandler: "+storiesToUpdate.size());
								
							}
							this.messagesBuffer.get(id).clear(); //clear chunks buffer for this message.
						}
					}
					/*
					 * First chunk arrived. We need to create a new Entry for our map
					 * with message id and a List for the chunks.
					 */
					else {
						ArrayList<Message> messagesforId = new ArrayList<Message>();
						messagesforId.add(msg);
						this.messagesBuffer.put(id, messagesforId);
						if(messagesBuffer.get(id).size() == msg.getNumOfChunks()) {
							MultimediaFile mmfile = new MultimediaFile(fileName,sender,msg.getBytes());
							if(header.equals("MMFile")) {
								System.out.println("Multimedia message arrived");
								Message m = new Message(header, messageTopic, mmfile, sender, fileName);
								topics.get(messageTopic).getMessageList().add(m);
							}
							else if(header.equals("Story")) {
								Story story=new Story(mmfile,sender);
								
								if(this.stories.containsKey(sender)) {
									stories.get(sender).add(story);
								}
								else {
									ArrayList<Story> newstories = new ArrayList<Story>();
									newstories.add(story);
									stories.put(sender,newstories);
								}
								this.storiesToUpdate.add(story);
								System.out.println("Clienthandler: "+storiesToUpdate.size());
							}
						}
						
					}
				}
				//request for list of stories available at the moment (Consumer).
				else if(header.equals("Get stories")) {
					String reply = "";
					for (Map.Entry<String, ArrayList<Story>> set : stories.entrySet()) {
						String uploader = set.getKey();
						String storiesForUploader = "";
						for(Story s: set.getValue())
							storiesForUploader += "\t"+s.getMMFile().getFileName()+"\n";
						if(!storiesForUploader.equals("")) {
							reply += uploader + "\n" + storiesForUploader;
						}
							
					}
					if(!reply.equals("")) {
						reply = reply.substring(0, reply.length() - 1);
					}
					Message replyMessage = new Message("Stories List", null, reply, sender);
					out.writeObject(replyMessage);
					out.flush();
					continue;
				}
				//request for a specific story, the mmfile that story contains (Consumer).
				else if(header.equals("Pick story")) {
					String[] storySenderName = msg.getData().split("\\s+");
					String uploader=storySenderName[0];
					String storyname=storySenderName[1];
					ArrayList<Story> storiesUploader =stories.get(uploader);
					boolean flag = false; //if client request a story after it has expired.
					for(Story s:storiesUploader) {
						if(storyname.equals(s.getMMFile().getFileName())) {
							flag = true;
							Message notifcationMessage = new Message("Story Time","","","");
							out.writeObject(notifcationMessage);
							out.writeObject(s.getDate());
							ArrayList<byte[]> chunks = Util.generateChunks(s.getMMFile());
							this.msgId++;
							int chunkSeq = 0;
							for(byte[] chunk : chunks) {
								Message m = new Message("Pick story reply", "" ,chunk,s.getSender(),s.getMMFile().getFileName(),this.msgId, chunks.size(),chunkSeq++);
								out.writeObject(m);
								out.flush();
							}	
						}
					}
					if(!flag) {
						Message m = new Message("Story has expired", "" ,"","");
						out.writeObject(m);
						out.flush();
					}
				}
				//simple text message that was uploaded in the conversation (Publisher).
				else if(header.equals("Text")) {
					System.out.println("Text message arrived");
					topics.get(messageTopic).getMessageList().add(msg);
				}
			} 
			catch (Exception e) {
				e.printStackTrace();
				break;
			} 		
		}
		/*
		 * If we received disconnection message
		 * loop will have break so we close the 
		 * connection and the streams.
		 */
		try {
			in.close();
			out.close();
			connection.close();
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

}
