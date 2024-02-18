import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;

/*
 * This class mainly stores the data of a multimedia
 * file (text or video) in a buffer. We will take care
 * of missing fields in the second phase where we will display
 * MMFile through android app.
 */

public class MultimediaFile implements Serializable{
	
	
	private String multimediaFileName;
	private String profileName;
	private String dateCreated;
	private String length;
	private String framerate;
	private String frameWidth;
	private String frameHeight;
	private byte[] buffer;
	public final String defaultMultimediaDirectory = "..\\multimedia\\";
	
	MultimediaFile(String fileName, String profileName){
		this.multimediaFileName = fileName;
		this.profileName = profileName;
		File file = new File(defaultMultimediaDirectory+fileName);
		buffer = new byte[(int) file.length()];
		this.read(file);
	}
	
	MultimediaFile(String fileName, String profileName,byte[] buffer){
		this.multimediaFileName = fileName;
		this.profileName 		= profileName;
		this.buffer 			= buffer;
	}
	
	public void read(File file) {
		try{
			FileInputStream inStream = new FileInputStream(file);
			inStream.read(buffer);
			inStream.close();
		}
		catch (Exception e){
		   e.printStackTrace();
		}
	}
	
	public String getFileName() {
		return this.multimediaFileName;
	}
	
	public int getLength() {
		return buffer.length;
	}
	
	public byte[] getData() {
		return this.buffer;
	}
	
	public String toString() {
		return this.multimediaFileName;
	}
}
