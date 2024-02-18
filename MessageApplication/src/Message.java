import java.io.Serializable;

/*
 * This is the class Message. It is used by 
 * nodes so the can communicate. Protocols can 
 * be implemented based on them.
 */
public class Message implements Serializable,Comparable<Message>{

	private String 			header;
	private String 			data;
	private String 			topic;
	private String 			fileName;
	private int 			id;
	private int 			numOfChunks;
	private String 			profileName;
	private byte[] 			bytes;
	private int 			sequence; //this field is needed for sorting the chunks of a specific message at the destination
	private MultimediaFile 	mmFile;
	private Story			story;
	
	public Message() {
		this.header 		= "";
		this.data 			= "";
		this.profileName 	= "";
		this.fileName 		= "";
	}
	
	public Message(String header, String topic, String data, String profileName) {
		this.header 		= header;
		this.topic 			= topic;
		this.data 			= data;
		this.profileName 	= profileName;	
	}
		
	public Message(String header, String topic, byte[] bytes, String profileName,String fileName ,int id, int numOfChunks, int sequence) {
		this.header 		= header;
		this.topic 			= topic;
		this.bytes 			= bytes;
		this.profileName 	= profileName;
		this.fileName 		= fileName; 
		this.id 			= id;
		this.numOfChunks 	= numOfChunks;
		this.sequence 		= sequence;
	}
	
	public Message(String header, String topic, MultimediaFile mmFile, String profileName,String fileName) {
		this.header 		= header;
		this.topic 			= topic;
		this.mmFile 		= mmFile;
		this.profileName 	= profileName;
		this.fileName 		= fileName; 
	}
	
	public Message(String header, Story story) {
		this.header 		= header;
		this.story 			= story;
	}

	public String getSender() {
		return this.profileName;
	}
	
	public String getData() {
		return this.data;
	}
	
	public byte[] getBytes() {
		return this.bytes;
	}
	
	public MultimediaFile getMMFile() {
		return this.mmFile;
	}
	
	public String getHeader() {
		return this.header;
	}
	
	public String toString() {
		if(this.header.equals("MMFile"))
			return this.profileName+": "+this.fileName;
		return this.profileName+": "+this.data;
	}
	
	public Story getStory() {
		return this.story;
	}
	
	public int getId() {
		return this.id;
	}
	
	public int getNumOfChunks() {
		return this.numOfChunks;
	}
	
	public String getTopic() {
		return this.topic;
	}
	
	public int getBytesLength() {
		return this.bytes.length;
	}
	
	public int getNoChunks() {
		return this.numOfChunks;
	}
	
	public int getSequence() {
		return this.sequence;
	}
	
	public String getFileName() {
		return this.fileName;
	}
	
	@Override 
	/*
	 * This is used in the reconstruction of a Multimedia file that has
	 * been fragmented. Sequence is the field that we increase for every 
	 * new chunk so by sorting them we can place in the correct order.
	 */
	public int compareTo(Message m) {
		return Integer.compare(this.sequence, m.sequence);
	}
	

}
