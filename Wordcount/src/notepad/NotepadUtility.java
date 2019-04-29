package notepad;

import fileManagment.FileManager;
import fileManagment.FileManagerException;

public class NotepadUtility {
	private static FileManager fileManager = new FileManager();
	public static void stringToDefaultNotepad(String line){
		try {
			fileManager.openFileWriter("/home/cloudera/Documents/HU/wordcount/src/wordlength", "fileWriteLines" , "txt");
			fileManager.writeLine("hallo");
			fileManager.closeFileWriter();
		} catch (FileManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
