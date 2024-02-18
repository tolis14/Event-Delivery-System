import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class Consumer implements Runnable {
	
	//Connections with Brokers
	ObjectInputStream[] 								in;
	ObjectOutputStream[] 								out;
	Socket[] 											connections;
	ProfileName 										profileName;
	
	/*
	 * This is used for buffering messages. We keep here the last
	 * 20 messages for every topic that consumer has received so 
	 * we can have very fast access to them. If consumer wants to see
	 * more messages then Show All method is implemented see later.
	 */
	private final HashMap<String,LinkedList<Message>> 	lastMessages;
	
	//Receiving the request from client
	private final ArrayBlockingQueue<Integer> 			requests;
	
	//Receiving messages from listeners
	private final ArrayBlockingQueue<Message> 			Qmsg;
	
	private final ArrayList<String> 					subtopics;
	private String 										activeTopic;
	private Thread[] 									listeners;
	
	public Consumer(String profileName, ArrayBlockingQueue<Integer> requests,ArrayList<String> subtopics) {
		this.connections 	= new Socket[Util.BROKERS_NUM];
		this.in 			= new ObjectInputStream[Util.BROKERS_NUM];
		this.out 			= new ObjectOutputStream[Util.BROKERS_NUM];
		this.listeners		= new Thread[Util.BROKERS_NUM];
		this.profileName 	= new ProfileName(profileName);
		this.requests 		= requests;
		this.Qmsg			= new ArrayBlockingQueue<Message>(20);
		this.activeTopic	= "";
		this.subtopics		= subtopics;
		this.lastMessages 	= new HashMap<String,LinkedList<Message>>();
	}
	
	/*
	 *This is the first connection. Instead of a default port
	 *for better randomization in the start we choose one to connect
	 * by select randomly one of the 3 brokers that exist. 
	 */
	public void connect() {
		// TODO Auto-generated method stub
		try {
			List<Integer> ports = Arrays.asList(6000,6001,6002);
			Random rand 				= new Random();
			int randomPort 				= ports.get(rand.nextInt(ports.size()));
			int index		 			= ports.indexOf(randomPort);
			this.connections[index] 	= new Socket("127.0.0.1",randomPort);
			this.out[index]    			= new ObjectOutputStream(connections[index].getOutputStream());
			this.in[index]     			= new ObjectInputStream(connections[index].getInputStream());
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	/*
	 * Connects to a specific port and IP address.
	 * Connection are kept open because we want to
	 * be able to receive all the traffic for topics
	 * that we are interested.
	 */
	private void connect(String ip, int port, int pos) {
		try {
			if(this.connections[pos]==null) {
				connections[pos] = new Socket(ip,port);
				this.out[pos]    = new ObjectOutputStream(connections[pos].getOutputStream());
				this.in[pos]     = new ObjectInputStream(connections[pos].getInputStream());
			}
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
	}

	
	/*
	 * This method is used in order to find
	 * the the port and the IP address of a Broker
	 * that is responsible for a topic. This method sends
	 * and Probe IP request to the 1st connection available. After
	 * sending the message it waits until a response is available
	 * and then it returns the Port and the IP address.
	 */
	private String[] findBroker(String topicName) {
		Message message = new Message("Probe IP", null, topicName, this.profileName.getProfileName());
		for(int i=0;i<this.connections.length;i++) {
			if(this.connections[i]!=null) {
				try {
					this.out[i].writeObject(message);
					Message m = Qmsg.take();
					while(!m.getHeader().equals("Probe IP Reply")) {
						Qmsg.add(m);
						m = Qmsg.take();
					}
					return m.getData().split("\\s+");
					
				} 
				catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}
	
	
	/*
	 * This method closes all the connections 
	 * and all the streams that the publisher has created.
	 * It's the last step before finish.
	 */
	public void disconnect() {
		// TODO Auto-generated method stub
		try {
			for(int i=0;i<this.connections.length;i++) {
				if(this.connections[i]!=null) {
					this.in[i].close();
					this.out[i].close();
					this.connections[i].close();
				}
			}
		}
		catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	
	void setActiveTopic(String activeTopic) {
		this.activeTopic = activeTopic;
	}
	
	/*
	 * This method is implementing the subscription functionality. If we already
	 * have opened a connection with the broker that is responsible for the topic 
	 * that we are interested then we can send him the request immediately. Otherwise,
	 * we probe the port and the IP of the broker responsible for the topic and we send a request.
	 * After receiving the data for the connection we connect and sending the request.
	 */
	
	void registerToTopic() {
		System.out.println("\nSelect a topic to register");
		String topicList= this.getTopics();
		Scanner input = new Scanner(System.in);
		String topicName = input.nextLine();
		if(!this.subtopics.contains(topicName) && topicList.contains(topicName)) {
			this.subtopics.add(topicName);
		}
		else {	
			return;
		}
		this.lastMessages.put(topicName, new LinkedList<Message>());
		
		Message register =  new Message("Register",null,topicName,this.profileName.getProfileName());
		try {
			int pos = Util.hashTopic(topicName);
			if(this.connections[pos]==null) {
				String[] ipPort = this.findBroker(topicName);
				this.connect(ipPort[0],Integer.parseInt(ipPort[1]),pos);
				this.createListener(pos);
			}
			out[pos].writeObject(register);	
			Message notify = new Message("Notify for new messages", null,"",this.profileName.getProfileName());
			out[pos].writeObject(notify);
		} 
		catch (IOException | NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * This method is creating a listener for 
	 * every new connection that is created.
	 */
	private void createListener(int pos) {
		Listener listener = new Listener(this.in[pos],this.Qmsg,this.lastMessages,this.profileName.getProfileName());
		Thread listenThread =  new Thread(listener);
		this.listeners[pos] = listenThread;
		this.listeners[pos].start();
	}
	
	
	/*
	 * This method sends a request to a Broker that he is connected with 
	 * in order to get all topics available at the moment.
	 */
	private String getTopics() {
		try {
			Message request =  new Message("Get topics",null," ",this.profileName.getProfileName());
			for(int i=0;i<this.connections.length;i++) {
				if(this.connections[i]!=null) {
					out[i].writeObject(request);
					break;
				}
			}
			Message m = Qmsg.take();
			while(!m.getHeader().equals("Topics List")) {
				Qmsg.add(m);
				m = Qmsg.take();
			}
			
			System.out.println("Available topics: ");
			System.out.println(m.getData());
			return m.getData();
		} 
		catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	/*
	 * This method is responsible to get the list 
	 * with the available stories. It finds the first Broker
	 * available and it sends him the request. Then it diplays for every user the 
	 * stories that he has uploaded and have not expired yet.
	 */
	private String getStories() {
		
		try {
			Message request =  new Message("Get stories",null," ",this.profileName.getProfileName());
			for(int i=0;i<this.connections.length;i++) {
				if(this.connections[i]!=null) {
					out[i].writeObject(request);
					break;
				}
			}
			Message m = Qmsg.take();
			while(!m.getHeader().equals("Stories List")) {
				Qmsg.add(m);
				m = Qmsg.take();
			}
			
			System.out.println("Available Stories: ");
			System.out.println(m.getData());
			return m.getData();
		} 
		catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/*
	 * This story calls the getStories (see above) internally
	 * and then it sends a request in the first Broker available to get the 
	 * story locally.
	 */
	private void displayStory() {
		String storiesList = getStories();
		if(storiesList.equals("")) {
			System.out.println("no stories");
			return;
		}
		System.out.println("Give username and story name: ");
		Scanner input = new Scanner(System.in);
		String story = input.nextLine();
		String[] storySenderName = story.split("\\s+");
		if(storiesList.contains(storySenderName[0]) && storiesList.contains(storySenderName[1])) {
			Message m=new Message("Pick story",null,story,this.profileName.getProfileName());			
			for(int i=0;i<this.connections.length;i++) {
				if(this.connections[i] != null) {
					try {
						out[i].writeObject(m);
						break;
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			try {
				Message msg = Qmsg.take();
				while(!(msg.getHeader().equals("Pick story reply") || msg.getHeader().equals("Story has expired"))) {
					Qmsg.add(msg);
					msg = Qmsg.take();
				}
				if(!msg.getHeader().equals("Story has expired"))
					System.out.println(msg.getStory());
				else
					System.out.println("This story has expired");
			}
			catch(InterruptedException e){
				e.printStackTrace();
			}
		}
	}
	
	
	/*
	 * This method asks from the broker the whole conversation
	 * for a specific topic. This is mandatory because we cannot hold in 
	 * our memory all the messages for all the topics. As described above, 
	 * we buffering last messages but if all conversation is asked,  we have to
	 * communicate with the broker. Of course we send the request directly to the 
	 * correct Broker using the hash function, because we are already connected with him.
	 */
	private void showConversationData(String topic) {
		try {
			Message requestConv = new Message("Get info",topic," ",this.profileName.getProfileName());
			int pos = Util.hashTopic(topic);
			out[pos].writeObject(requestConv);
			Message message = Qmsg.take();
			while(!message.getHeader().equals("Topic conversation")) {
				Qmsg.add(message);
				message = Qmsg.take();
			}
			ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(message.getBytes()));
			ArrayList<Message> convData = (ArrayList<Message>)ois.readObject();
			for (Message m: convData) {
				System.out.println(m);
			}
		} 
		catch (IOException | ClassNotFoundException | InterruptedException | NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * This method displays in the console the last 20 
	 * messages (those are buffered) for the topic requested.
	 */
	private void showLastMessages(String topicName) {
		for(Message m: this.lastMessages.get(topicName)) {
			System.out.println(m);
		}
	}
	
	/*
	 * This method informs the Broker that our publisher
	 * is going to end the connection. After sending that message
	 * connections are about to close without problems.
	 */
	public void sendDisconnectionMessage() {
		Message leavingMessage = new Message("Disconnection", null, "leave",this.profileName.getProfileName());
		try {
			for(int i=0;i<this.out.length;i++)
				if(this.out[i]!=null)
					this.out[i].writeObject(leavingMessage);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Main loop of the consumers. Here he takes requests from the 
	 * client and given that executes the correct command.
	 */
	@Override
	public void run() {

		this.connect();
		System.out.println("Consumer running...");

		for(int i=0;i<this.connections.length;i++) {
			if(this.connections[i]!=null) {
				Listener listener = new Listener(this.in[i],this.Qmsg,this.lastMessages,this.profileName.getProfileName());
				this.listeners[i] = new Thread(listener);
				this.listeners[i].start();
				break;
			}
		}
		
		while(!Thread.currentThread().isInterrupted()) {
			try {
					
				int task = -1;
				if(!this.requests.isEmpty()) {
					task = this.requests.take(); // waits until a request is produced by the client
				}
				if(task == 2) {
					
					int flag=0;
					this.showLastMessages(this.activeTopic);
					Message notify = new Message("Notify for new messages", null,"",this.profileName.getProfileName());
					long start = System.nanoTime(); 
					while(!this.activeTopic.equals("back")) { 
						long elapsedTime = System.nanoTime() - start;
						int pos = Util.hashTopic(activeTopic);
						if(elapsedTime>=1000000000) {
							start = System.nanoTime(); 
							out[pos].writeObject(notify);
							out[pos].flush();
						}
						for(Message m : this.Qmsg) {
							if(m.getTopic().equals(this.activeTopic) ) {
								if(flag==1) { // if flag is 0 , its the 1st time opening the message dialog so we skip that part.
									System.out.println(m);
								}
								this.Qmsg.remove(m);
							}
						}
						if(MessengerClient.showAll) {
							this.showConversationData(this.activeTopic);
							MessengerClient.showAll = false;
						}
						if(flag==0) {
							flag=1;
						}
					}
				}
				else if(task == 3) {
					this.registerToTopic();
				}
				else if(task == 5) {
					this.displayStory();
				}
				else if(task == 0) {
					this.sendDisconnectionMessage();
					break;
				}

				if(task!=-1 && task!=2)
					MessengerClient.threadFinished = true;
			} 
			catch (InterruptedException | IOException | NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			
		}
		
		//Stop all listeners.
		for(Thread t: this.listeners) {
			if(t!=null)
				t.interrupt();
		}
		System.out.println("Consumer disconnect");
		this.disconnect();
	}

}
