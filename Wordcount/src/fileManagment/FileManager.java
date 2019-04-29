package fileManagment;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;


/**
 * Can only be used for text related filetypes. For example .java, .txt.
 * This class cann't write or read images etc (not text related).
 * 
 */
public class FileManager{
	private BufferedWriter bufferedWriter;
	private BufferedReader bufferedReader;
	
	/**
	   * Opens a write connection with a file.
	   * @param fileType filetype without the dot (example .exe)
	   * @param fileName name of the file
	   * @exception FileManagerException
	   * 	if specified file is a directory or some unknown error has occured
	   */
	public void openFileWriter(String fileName, String fileType) throws FileManagerException{
		openFileWriter("", fileName, fileType);
	}
	
	/**
	 * @param fileLocation example "C:\Users\Frank\Documents\HU\OwnProjects\UMLParser"
	 * @see #openFileWriter(fileName, fileType)
	 */
	
	public void openFileWriter(String fileLocation , String fileName, String fileType) throws FileManagerException{
		String filePath = "";
		if (!fileLocation.equals("")){
			filePath+=fileLocation+"/";
		}
		filePath+=fileName+"."+fileType;
		
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(filePath));
		} catch (IOException e) {
			throw new FileManagerException(filePath+"The specified file is a directory, or can not be created, the specified map does not exist or opened for any other reason"+e.getMessage());
		}
	}
	
	/**
	 * 
	 * @param fileName name of the file
	 * @param fileType filetype, (example java, so not .java)
	 * @throws FileManagerException
	 */
	
	public void openFileReader(String fileName, String fileType) throws FileManagerException{
		openFileReader("", fileName, fileType);
	}
	
	/**
	   * Opens a write connection with a file.
	   * @param filePath / at the end is not needed, "" if there is no patch needed.
	   * @exception FileManagerException
	   * 	if specified file is a directory, does not exist or some unknown error has occured
	   * @see #openFileReader(fileName, fileType)
	   */
	public void openFileReader(String filePath, String fileName, String fileType) throws FileManagerException{
		   try {
			if ("".equals(filePath)){
				bufferedReader = new BufferedReader(new FileReader(fileName+"."+fileType));
			}
			else {
				FileReader f = new FileReader(filePath+"/"+fileName+"."+fileType);
				bufferedReader = new BufferedReader(f);
			}
			} catch (FileNotFoundException e) {
			throw new FileManagerException("The specified file is a directory, does not exist or can not be opened for any other reason, Message:" + e.getMessage());
		}
	}
	
	/**
	   * writes text to a file.
	   */
	public void write(String text) throws FileManagerException{
		try {
			bufferedWriter.write(text);
		} catch (IOException e) {
			throw new FileManagerException("an IOException seems to have occured");
		} catch (NullPointerException e){
			throw new FileManagerException("A fileWriter connection does not seem to be open");
		}
	}
	
	/**
	   * writes text to a file and the writer does an enter
	   * won't work if the connection is not closed afterwards.
	   * @exception FileManagerException
	   *   possible can be if a fileWriter is not open
	   */
	public void writeLine(String text) throws FileManagerException{
		try {
			bufferedWriter.write(text);
	        bufferedWriter.newLine();
		} catch (IOException e) {
			throw new FileManagerException("an IOException seems to have occured");
		} catch (NullPointerException e){
			throw new FileManagerException("A fileWriter connection does not seem to be open");
		}
	}
	
	/**
	   * Closes a write connection with a file.
	   * @exception FileManagerException
	   *   if the connection was not open to begin with
	   */
	public void closeFileWriter() throws FileManagerException{
		try {
			bufferedWriter.close();
		} catch (IOException e) {
			throw new FileManagerException("An IOException seems to have occured");
		} catch (NullPointerException e){
			throw new FileManagerException("The fileWriter is already closed");
		}
	}
	
	/**
	   * reads text from an file.
	   * @return a line of text found in the specified document
	   * @exception FileManagerException
	   *   possible can be if a fileWriter is not open
	   */
	public String readLine() throws FileManagerException{
		try {
			return bufferedReader.readLine();
		} catch (IOException e) {
			throw new FileManagerException("An IOException seems to have occured");
		} catch (NullPointerException e){
			throw new FileManagerException("A fileWriter connection does not seem to be open");
		}
	}
	
	/**
	   * Closes a read connection with a file.
	   * @exception FileManagerException
	   *   if the connection was not open to begin with
	   */
	public void closeFileReader() throws FileManagerException{
		try {
			bufferedReader.close();
		} catch (IOException e) {
			throw new FileManagerException("An IOException seems to have occured");
		} catch (NullPointerException e){
			throw new FileManagerException("The fileReader is already closed");
		}
	}
	
	/**
	   * finalize is called if the object is destroyed. The garbargecollector is however unreliable with
	   * destroying objects. You never know when it happens. Do not rely on this method to close your connections.
	   * It is just a backup in case it happens as there is a good chance but no guarantee that the object will be
	   * destroyed and thus this method called
	   * only the garbagecollector is supposed to use this method
	   */

	protected void finalize(){
		//can not been thrown as there will be a memory leak if that happens
		try{
			closeFileReader();
		}
		catch(Exception e){}//nullpointer and filemanager included
		try{
			closeFileWriter();
		}
		catch(Exception e){}//nullpointer and filemanager included
	}
}