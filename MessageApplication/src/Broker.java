import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * Broker class is representing the internal nodes of the 
 * Event delivery system. They are responsible for accepting 
 * connections with clients and then start a Thread for each of 
 * them in order to be able to serve multiple clients at the same 
 * time. Every Broker communicates with every other Broker in the 
 * system. Their job is to store a number of topics, take  messages from the publishers and 
 * automatically forwarding them to the subscribers who are interested in such topics.
 */

public class Broker {
	
	public ServerSocket 								serverSocket;
	private final HashMap<String,Topic> 				topics; //topics that this Broker is responsible for.
	private volatile ArrayList<String> 					topicNames; //Names of topic that this Broker is responsible for used by threads.
	private volatile HashMap<Integer,ArrayList<String>> topicsPerBroker; // Holds every Broker node's ID and the topics that he is responsible for.
	private final HashMap<Integer,String>				defaultTopics; //Initializes a default topic for this broker
	private int port;
	
	//Incoming connections and streams with other every other Broker.
	private ObjectInputStream[] 	incIn;
	private ObjectOutputStream[] 	incOut;
	private Socket[] 				incConnections;
	
	////Out coming connections and streams with other every other Broker.
	private ObjectOutputStream[] 	outOut;
	private ObjectInputStream[] 	outIn;
	private Socket[]    			outConnections;
	
	private final ConcurrentHashMap<String,ArrayList<Story>> stories; //this Map needs update between threads all the time so concurrent will help
	private volatile ArrayList<Story> storiesToUpdate;


	public Broker(int port){
		this.topics 			= new HashMap<String,Topic>();
		this.topicNames 		= new ArrayList<String>();
		this.topicsPerBroker 	= new HashMap<Integer,ArrayList<String>>();
		this.defaultTopics 		= new HashMap<Integer,String>();
		this.stories			= new ConcurrentHashMap<String,ArrayList<Story>>();
		this.storiesToUpdate	= new ArrayList<Story>();

		
		//hard coded default topics.
		this.defaultTopics.put(6000,"texnologia logismiku");
		this.defaultTopics.put(6001,"skyliii");
		this.defaultTopics.put(6002,"katanemimena systimata");
		
		try {
			this.port = port;
			this.incConnections = new Socket[Util.BROKERS_NUM -1];
			this.outConnections = new Socket[Util.BROKERS_NUM -1];
			this.incIn 			= new ObjectInputStream[Util.BROKERS_NUM - 1];
			this.incOut 		= new ObjectOutputStream[Util.BROKERS_NUM - 1];
			this.outIn 			= new ObjectInputStream[Util.BROKERS_NUM - 1];
			this.outOut 		= new ObjectOutputStream[Util.BROKERS_NUM - 1];
			serverSocket 		= new ServerSocket(this.port);
			System.out.println("Server Started");
		} 
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/*
	 * This method is the main loop of the Broker.
	 * Initially we create Speakers and Listeners so we 
	 * can communicate with other Brokers silently in the
	 * background for events that occurs such as modification to
	 * topics
	 */
	private void connectWithClients() {
			
		while (!serverSocket.isClosed()) {
			try {
				Socket connection = serverSocket.accept();
				System.out.println("A new user has connected\n");
				ClientHandler client = new ClientHandler(connection, this.topics, this.topicNames,this.topicsPerBroker,stories,this.storiesToUpdate);
				Thread t = new Thread(client);
				t.start();
			} 
			catch (IOException e) {
				this.disconnect();
			}
		}
	}
	
	
	/*
	 * This method is used in order to accept the connections
	 * from every other Broker of the Brokers cluster.
	 * It also initializes all streams for the incoming connections.
	 */
	private void acceptFromBrokers() {
		int k = 0;
		while(true) {
			try {
				Socket connection = serverSocket.accept();
				this.incConnections[k] = connection;
				this.incOut[k] = new ObjectOutputStream(connection.getOutputStream());
				this.incIn[k] = new ObjectInputStream(connection.getInputStream());
				System.out.println("A new Broker has connected\n");
				
				if(k==Util.BROKERS_NUM - 2)
					break;		
				k++;
			} 
			catch (IOException e) {
				this.disconnect();
			}
		}
	}
	
	/*
	 * This method is used in order to connect with every other Broker
	 * in the Brokers cluster. It also initializes all streams for 
	 * the out coming connections.
	 */
	private void connectWithBrokers(int k, String ip, int port) {
		try {
			this.outConnections[k]  = new Socket(ip,port);
			this.outOut[k] 			= new ObjectOutputStream(outConnections[k].getOutputStream());
			this.outIn[k] 			= new ObjectInputStream(outConnections[k].getInputStream());
		} 
		catch (IOException e) {
			this.disconnect();
		}
	}

	//Closes every stream and every connection between this broker and every other broker.
	private void disconnect() {
		try {
			for(int i=0;i<this.outConnections.length;i++) {
				if(this.outConnections[i]!=null) {
					this.outOut[i].close();
					this.outIn[i].close();
					this.outConnections[i].close();
				}
			}
			for(int i=0;i<this.incConnections.length;i++) {
				if(this.incConnections[i]!=null) {
					this.incOut[i].close();
					this.incIn[i].close();
					this.incConnections[i].close();
				}
			}	
		} 
		catch (IOException e) {
				
		}
	}
	
	
	/*
	 * Starting listeners and notifiers of the Broker.
	 * Used for communication with other brokers.
	 * Listeners will receive messages from other brokers,
	 * while speakers will send messages to other brokers.
	 */
	private void startThreads() {
		for(int i=0;i<Util.BROKERS_NUM-1;i++) {
			BrokerListener bl 	= new BrokerListener(incIn[i],this.topicsPerBroker,this.stories);
			Thread t 			= new Thread(bl);
			t.start();
		}
		
		for(int i=0;i<Util.BROKERS_NUM-1;i++) {
			BrokerSpeaker bs 	= new BrokerSpeaker(outOut[i],this.topicNames,this.port,this.storiesToUpdate);
			Thread t 			= new Thread(bs);
			t.start();
		}
		
		StoriesUpdateHandler storiesHandler = new StoriesUpdateHandler(this.stories);
		Thread storiesHandlerThread 		= new Thread(storiesHandler);
		storiesHandlerThread.start();
	}
	
	
	/*
	 * Initialize default topics for this broker and other brokers.
	 * Initialization is being via brokers communication (see BrokerSpeaker, 
	 * BrokerListener classes).
	 */
	private void initTopics(int port)  {
		String topicName=this.defaultTopics.get(port);
		this.topicNames.add(topicName);
		this.topics.put(topicName, new Topic(topicName));			
	}
		

	
	/*
	 * This method is running the Broker.Initially
	 * it creates all the connections with other Brokers
	 * and then calls the connectWithClients method so the 
	 * Broker main loop starts.
	 */
	private static void run(int port) throws IOException, InterruptedException {
		Broker broker = new Broker(port);
		BufferedReader reader = new BufferedReader(new FileReader("..\\config.txt"));
		int i = 0;
		int k = 0;
		while(i <= Util.BROKERS_NUM-1 ) {
			String[] line = reader.readLine().split("\\s+");
			String ip = line[0];
			int p = Integer.parseInt(line[1]);
			if (p==port) {
				System.out.println("Accept connections");
				broker.acceptFromBrokers();
			}
			else {
				System.out.println("Create connections");
				broker.connectWithBrokers(k,ip,p);
				k++;
			}
			i++;
		}
		reader.close();
		broker.initTopics(port);
		broker.startThreads();
		broker.connectWithClients();
	}
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		int port = Integer.parseInt(args[0]);
		Broker.run(port);
	}
}
