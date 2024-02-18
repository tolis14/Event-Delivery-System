import java.io.File;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

/*
 * This is our client, typically a phone device.
 * It has 2 threads one is the publisher which sending
 * messages and creates topics and other is the consumer which
 * is responsible for taking the messages for the topics in 
 * which the user has subscribed. It has a simple command line UI menu
 * from which the user can access the functionality that this class provides.
 * Phase 2 for a better UI tho :P
 */

public class MessengerClient {

	private Publisher 							publisher;
	private Consumer  							consumer;
	private ProfileName 					 	profileName;
	
	/*
	 * These to Blocking Queues are used for the communication between 
	 * the client and its threads. In these queues, client stores his 
	 * requests (the tasks) that every thread should execute.
	 */
	private final ArrayBlockingQueue<Integer> 	requestsForPublisher;
	private final ArrayBlockingQueue<Integer> 	requestsForConsumer;
	
	private final ArrayList<String> subtopics;
	
	/*
	 *After assigning  a job to a thread we have to wait until
	 *the job is done before we ask for another task. Hence we have to
	 *wait and this variable helps with it.
	 */
	public static volatile boolean threadFinished = false;
	
	/*
	 * This variable is used in order to 
	 * implement the Show All functionality in 
	 * the conversation area.
	 */
	public static volatile boolean showAll = false;
	
	
	public MessengerClient(String profileName) {
		this.profileName = new ProfileName(profileName);
		this.requestsForPublisher 	= new ArrayBlockingQueue<Integer>(1);
		this.requestsForConsumer  	= new ArrayBlockingQueue<Integer>(1);
		this.subtopics   			= new ArrayList<String>();
		this.publisher	 			= new Publisher(profileName, this.requestsForPublisher,this.subtopics); 
		this.consumer 	 			= new Consumer (profileName, this.requestsForConsumer,this.subtopics);
		this.initDirectory();
	}
	
	
	/*
	 * Creates a directory where we store MMfile incoming
	 */
	
	private void initDirectory() {
		String path = "..\\ClientsDirectories\\";
		File directory = new File(path+this.profileName.getProfileName());
		directory.mkdir();
	}
	
	/*
	 * This is the main method of this class
	 * It provides user with a simple command line 
	 * interface so they can use application's functionality.
	 */
	public static void main(String[] args) throws InterruptedException {

		String profileName 			= args[0];
		MessengerClient client 		= new MessengerClient(profileName);

		Thread publisher_thread 	= new Thread(client.publisher);
		Thread consumer_thread 		= new Thread(client.consumer);
		publisher_thread.start();
		consumer_thread.start();
		
		Scanner input = new Scanner(System.in);
		int choice=-1;
		
		
		while(true) {			
			boolean validinput = false;
	        while (!validinput) {
	            try {

	    			System.out.println("\n1.Create topic");
	    			System.out.println("2.Send message");
	    			System.out.println("3.Subscribe to a topic");
	    			System.out.println("4.Upload story");
	    			System.out.println("5.Show stories");
	    			System.out.println("0.Exit");
	                choice = input.nextInt();
	                if(choice>=0 && choice<=5)
	                	validinput = true;
	                else 
	                {
	                	System.out.println("\nInvalid input type must be an integer between 0-5");
	                }
	            }
	            catch (InputMismatchException e) {
	                System.out.println("\nInvalid input type (must be an integer)"); 
	                choice = -1;
	                input.nextLine();

	            }
	        }
			
			input.nextLine();
			if(choice == 1 || choice == 2 || choice==4) {
				client.requestsForPublisher.add(choice);
				if(choice == 2) {
					/*
					 * The busy waiting here is used in order to wait
					 * for the active topic to be set by the Publisher.
					 * Publisher after finishing with that will add a 
					 * message to this queue so we can continue.
					 */
					while(!client.requestsForPublisher.isEmpty());
					client.requestsForPublisher.take();// publisher has set the active topic, update consumer
					String activeTopic = client.publisher.getActiveTopic();
					if(!activeTopic.equals("wrongTopic")) {
						client.consumer.setActiveTopic(activeTopic);
						client.requestsForConsumer.add(choice);
					}
				}
				
			}
			else if(choice == 3 || choice == 5) {
				client.requestsForConsumer.add(choice);
			}		
			else if(choice == 0) {
				client.requestsForPublisher.add(choice);
				client.requestsForConsumer.add(choice);
				break;
			}
			
				
			while(!threadFinished) {
				//busy waiting
			}
			threadFinished = false;	
			client.consumer.setActiveTopic("back");
		}
		input.close();
	}
	
}
