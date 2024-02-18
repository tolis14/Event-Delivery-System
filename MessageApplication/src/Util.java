import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;


/*
 * Util class. This class holds some functionality such as
 * hashing and chunking which are used by many componets of
 * the Event delivery system.
 */
public class Util {
	
	public final static int BROKERS_NUM = 3;
	
	//calculates the remain bytes offset of file.
	private static int calc(int remain,int CHUNK_SIZE) {
		 if(remain-Publisher.CHUNK_SIZE>0){
			 remain-=Publisher.CHUNK_SIZE;
		 }
		 return remain;
	}
	
	//this method is used in order to generate multimedia's file chunks.
	static ArrayList<byte[]> generateChunks(MultimediaFile mFile){

		int offset=0;
		int fileSize = mFile.getLength();
		int remain=fileSize;
		
		ArrayList<byte[]> chunks = new ArrayList<byte[]>();
	 
		while(true){
			
			remain=calc(remain,Publisher.CHUNK_SIZE); 
			
			if(Publisher.CHUNK_SIZE+offset>=fileSize-1) {
				byte[] lastChunk = new byte[remain];
			    for (int i = offset; i < offset+remain; i++)
			    	lastChunk[i-offset] = mFile.getData()[i];
			    chunks.add(lastChunk);
			    break;
			}
			 
			byte temp[] = new byte[Publisher.CHUNK_SIZE];
		    for (int i = offset; i < offset+Publisher.CHUNK_SIZE; i++)
		    	temp[i-offset] = mFile.getData()[i];
		    
		    chunks.add(temp); 
			offset=offset+Publisher.CHUNK_SIZE; 	 
		}
		return chunks;
	}
	
	/*
	 * this method gets as input the name of the topic and by using MD5 algorithm 
	 * generates a number in range [0,2] so we can load balancing between the 3 brokers
	 * that are used in our application
	 */
	public static int hashTopic(String topicName) throws UnsupportedEncodingException, NoSuchAlgorithmException{
		byte[] bytesOfMessage = topicName.getBytes("UTF-8");
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] theMD5digest = md.digest(bytesOfMessage);
		int value = new BigInteger(theMD5digest).intValue();
		return Math.abs(value) % 3;
	}
	
	/*
	 * This method is used to get the IP and the port for a specific node 
	 * which are stored in the configuration file.
	 * The method takes as input the number of the broker that we want to locate
	 * and it returns as the IP and the port as strings. 
	 */
	public static String[] findIpPort(String topicName) {
		try {
			int broker = Util.hashTopic(topicName);
			BufferedReader reader = new BufferedReader(new FileReader("..\\config.txt"));
			String line = "";
			for(int i=0; i<=broker; i++) {
				line = reader.readLine();	
			}
			String ipAndPort[] = line.split("\\s+");
			reader.close();
			return ipAndPort;
		} 
		catch (IOException | NoSuchAlgorithmException e) {
			e.printStackTrace();
		}	
		return null;
	}
	
}
