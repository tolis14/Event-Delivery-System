import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.io.*;
import java.net.Socket;

public class Publisher implements Runnable {
	
	//full path is required so we can run it via cmd without errors.
	//We assume that every phone has a default directory for the multimedia files.
	public final String defaultMultimediaDirectory = "..\\multimedia";
	
	public final static int CHUNK_SIZE = 524288; // 512 * 1024 = 512KB chunk size
	
	private ObjectInputStream 					in;
	private ObjectOutputStream					out;
	private Socket 								connection;
	private ProfileName 						profileName;
	private int 								msgId;
	private final ArrayBlockingQueue<Integer> 	requests;
	private final ArrayList<String> 			subtopics;
	private String 								currentTopic;
	
	public Publisher(String profileName, ArrayBlockingQueue<Integer> requests, ArrayList<String> subtopics) {
		this.profileName  	= new ProfileName(profileName);
		this.requests 	  	= requests;
		this.subtopics	  	= subtopics;
		this.currentTopic 	= "";
		this.msgId 			= 0;
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
			Random rand 		= new Random();
			int randomPort 		= ports.get(rand.nextInt(ports.size()));
			this.connection 	= new Socket("127.0.0.1",randomPort);
			this.out    		= new ObjectOutputStream(connection.getOutputStream());
			this.in     		= new ObjectInputStream(connection.getInputStream());
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
	}
	
	/*
	 * Connects to a specific port and IP address.
	 */
	private void connect(String ip, int port) {
		try {
			this.connection = new Socket(ip,port);
			this.out    	= new ObjectOutputStream(connection.getOutputStream());
			this.in     	= new ObjectInputStream(connection.getInputStream());
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * This method is used in order to find
	 * the the port and the IP address of a Broker
	 * that is responsible for a topic. This method sends
	 * and Probe IP request. After sending the message it waits 
	 * until a response is available and then it returns the 
	 * Port and the IP address.
	 */
	private String[] findBroker(String topicName) {
		Message message = new Message("Probe IP", null, topicName, this.profileName.getProfileName());
		try {
			this.out.writeObject(message);
			Message reply = (Message)this.in.readObject();
			return reply.getData().split("\\s+");
		} 
		catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	

	public String getActiveTopic() {
		return this.currentTopic;
	}
	

	/*
	 * This method display in the console all files that are 
	 * stored in the multimedia directory.
	 */
	public String displayMultimediaDirectory() {
		File directory = new File(this.defaultMultimediaDirectory);
		String str = "";
		for(String s: directory.list()) {
			System.out.println(s);
			str+=s+" ";
		}
		return str;
	}
	
	
	/*
	 * This method is used in order for a user to be 
	 * able to create new topics. First it takes the input from the
	 * user for the topic that he wants to create. Then it uses the findIpPort
	 * method to find the broker responsible for the topic. Then it closes the current connection
	 * and opens a new one in order to create the topic.
	 */
	private void createTopic()  {
		Scanner input = new Scanner(System.in);
		System.out.println("Give topic name");
		String topicName = input.nextLine();
		
		String[] ipPort = this.findBroker(topicName);
		this.sendDisconnectionMessage();
		this.disconnect();
		this.connect(ipPort[0], Integer.parseInt(ipPort[1]));
		
		try {
			Message message = new Message("Create topic", null, topicName, this.profileName.getProfileName());
			this.out.writeObject(message);
			Message reply=(Message)this.in.readObject();
			System.out.println(reply.getData());
		} 
		catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * This method displays in the console all the
	 * topics that we are registered in.
	 */
	private void showAvailTopics() {
		for(String s: this.subtopics) {
			System.out.println(s);
		}
	}
	
	
	/*
	 * This method is used in order for our client to send messages
	 * to the topics in which he has registered. If the file is multimedia
	 * it generates the chunks and send them all 1 by 1. If the message is 
	 * just a pure text then no chunking is required so it just sends a String.
	 * First of all it sends an IP port request to find the responsible broker 
	 * for this topic, then it closes the current connection and finally it connects to the 
	 * correct broker to push him the message.
	 */
	public void sendToTopic() throws InterruptedException{
		Scanner input = new Scanner(System.in);
		System.out.println("Select topic:");
		this.showAvailTopics();
		String topicName = input.nextLine();
		if(!this.subtopics.contains(topicName)) {
			this.currentTopic="wrongTopic";
			this.requests.add(6);
			while(this.requests.size()==1) { //busy waiting until the client sets the active topic for consumer.
				
			}
			return;
		}
		this.currentTopic = topicName;
		
		String[] ipPort = this.findBroker(topicName);
		this.sendDisconnectionMessage();
		this.disconnect();
		this.connect(ipPort[0], Integer.parseInt(ipPort[1]));
		
		
		//WAKES UP CLIENT!!!!!!!!
		this.requests.add(6);
		
		Thread.sleep(200);
		System.out.println("Press Multimediafile to send multimedia files.");
		System.out.println("Press ShowAll to see all the messages of the topic");
		System.out.println("Press back to return to menu");
		System.out.println("Give message ("+topicName+"): ");
		
		
		while(true) {
			String data =  input.nextLine();
			if(data.equals("Multimediafile")) {
				String str=this.displayMultimediaDirectory();
				String choice=input.nextLine();
				String fileName = choice;
				if(!str.contains(choice)) 
				{
					System.out.println("This file does not exist");
					continue;
				}
				MultimediaFile mediaFile = new MultimediaFile(fileName, this.profileName.getProfileName());
				ArrayList<byte[]> chunks = Util.generateChunks(mediaFile);
				this.msgId++;
				int chunkSeq = 0;
				for(byte[] chunk : chunks) {
					Message msg = new Message("MMFile", topicName,chunk,this.profileName.getProfileName(),fileName,this.msgId, chunks.size(),chunkSeq++);
					this.push(msg);
				}	
			}
			else if(data.equals("ShowAll")) {
				MessengerClient.showAll = true;
			}
			else if (data.equals("back")){
				break;
			}
			else {
				Message msg = new Message("Text", topicName, data, this.profileName.getProfileName());
				this.push(msg);
			}
		}
	}
	
	
	/*
	 * This method is responsible for the story upload. It sends to the broker that our
	 * client is connected at the moment a story. The story is containing a multimedia
	 * file so the transfer is being with chunks.
	 */
	private void uploadStory() {
		
		Scanner input = new Scanner(System.in);
		String str = this.displayMultimediaDirectory();
		String choice = input.nextLine();
		String fileName = choice;
		
		if(!str.contains(choice)) 
		{
			System.out.println("This file does not exist");
			return;
		}
		MultimediaFile mediaFile = new MultimediaFile(fileName, this.profileName.getProfileName());
		ArrayList<byte[]> chunks = Util.generateChunks(mediaFile);
		this.msgId++;
		int chunkSeq = 0;
		for(byte[] chunk : chunks) {
			Message msg = new Message("Story", "" ,chunk,this.profileName.getProfileName(),fileName,this.msgId, chunks.size(),chunkSeq++);
			this.push(msg);
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
			this.out.writeObject(leavingMessage); 
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * This method takes a message and pushes it to the 
	 * Broker responsible for this message.
	 */
	private void push(Message msg) {
		try {
			this.out.writeObject(msg);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * This is the main loop of the publisher. Given the request that
	 * he is taking from the client, the publisher executes it.
	 */
	@Override
	public void run() {
		this.connect();
		System.out.println("Publisher running...");
		
		while(!Thread.currentThread().isInterrupted()) {
			
			try {
				int task = this.requests.take(); //thread will block until a request is produced by client.
				
				if(task == 1) {
					this.createTopic();
				}
				else if(task == 2) {
					this.sendToTopic();
				}
				else if(task ==4) {
					this.uploadStory();
				}
				else if(task == 0) {
					this.sendDisconnectionMessage();
					break;
				}
				MessengerClient.threadFinished = true;
			} 
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		
		}
		System.out.println("Publisher disconnect");
		this.disconnect();
	}
	
	
	/*
	 * This method closes all the connections 
	 * and all the streams that the publisher has created.
	 * It's the last step before finish.
	 */
	public void disconnect() {
		// TODO Auto-generated method stub
		try {
			this.in.close();
			this.out.close();
			this.connection.close();
		}
		catch (IOException e) {
			
			e.printStackTrace();
		}
	}	
}