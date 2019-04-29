package wordlength;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;



public class Wordlength extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(Wordlength.class);
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Wordlength(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private boolean caseSensitive = false;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

    protected void setup(Mapper.Context context)
      throws IOException,
        InterruptedException {
      Configuration config = context.getConfiguration();
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
    }

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      NotepadUtility.stringToDefaultNotepad(line+"hi");
      if (!caseSensitive) {
        line = line.toLowerCase();
      }
      Text currentWord = new Text();
        for (String word : WORD_BOUNDARY.split(line)) {
          if (word.isEmpty()) {
            continue;
          }
          currentWord = new Text(word.length()+"");
          context.write(currentWord,one);
        }
      }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}

class NotepadUtility {
	private static FileManager fileManager = new FileManager();
	public static void stringToDefaultNotepad(String line){
		try {
			//fileManager.openFileWriter("/home/cloudera/Documents/HU/wordcount/src/wordlength", "fileWriteLines" , "txt");
			fileManager.openFileWriter("/user/cloudera/wordcount/logs", "fileWriteLines" , "text");
			fileManager.writeLine("hallo");
			fileManager.closeFileWriter();
		} catch (FileManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

/**
 * Can only be used for text related filetypes. For example .java, .txt.
 * This class cann't write or read images etc (not text related).
 */
class FileManager{
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

class FileManagerException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FileManagerException(String message){
		super(message);
	}
}


