import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Story implements Serializable{
	
	private MultimediaFile 		mmfile;
	private Date 				date;
	private String 				sender;
	private SimpleDateFormat	formatter;

	public Story(MultimediaFile mmfile,String sender){
		
		this.mmfile=mmfile;
		this.date= new Date();
		this.formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		this.sender=sender;
	}
	
	public Story(MultimediaFile mmfile,String sender,Date date){
		
		this.mmfile= mmfile;
		this.date  = date;
		this.sender= sender;
		this.formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
	}
	
	public Date getDate() {
		return this.date;
	}
	
	public String getSender() {
		return this.sender;
	}
	
	public MultimediaFile getMMFile() {
		return this.mmfile;
	}
	
	public String toString() {
		return "Uploaded by: " +sender+"\n"+formatter.format(this.date) +"\n"+mmfile.getFileName();
	}
	
}

