package pagerank;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

public class Pagerank extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Pagerank.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Pagerank(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	      JobControl jobControl = new JobControl("jobChain");
	      System.out.println("vlak voor de nullpointers");
	      ControlledJob previousControlledJob = null;
	      Job job = null;
	      System.out.println("vlak na de nullpointers");
		  URI skipUri = null;
		  //pagerank draait dan 10x
		  for (int j = 0;j<9;j++){
			  Configuration conf = getConf();
			  System.out.println("Current step: "+j);
			  
			  job = Job.getInstance(conf, "pagerankstap"+j);
			  if(j == 0){
				  System.out.println("skipuri wordt aangemaakt");
				  for (int i = 0; i < args.length; i += 1) {
				      if ("-skip".equals(args[i])) {
				        job.getConfiguration().setBoolean("wordcount.skip.nodeRelations", true);
				        i += 1;
				        skipUri = new Path(args[i]).toUri();
				        job.addCacheFile(skipUri);
				    
				        // this demonstrates logging
				        LOG.info("Added file to the distributed cache: " + args[i]);
				      }
				    }
				  System.out.println("skipuri succesfull aangemaakt");
			  }
			  else{
				  job.getConfiguration().setBoolean("wordcount.skip.nodeRelations", true);
				  job.addCacheFile(skipUri);
				  System.out.println("skipuri succesfull geimplementeerd");
				  //volgensmij gaat dit goed in asynchroon, ook als je niet meerekend dat alle stappen dependend op elkaar zijn.
			  }
			  
			  job.setJarByClass(this.getClass());
			  if (j == 0){
				  FileInputFormat.setInputPaths(job, new Path(args[0]));
				  FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp0"));
			  }
			  else if (j == 9){
				  FileInputFormat.setInputPaths(job, new Path(args[1] + "/temp"+(j-1)));
				  FileOutputFormat.setOutputPath(job, new Path(args[1] + "/final"));
			  }
			  else{
				  FileInputFormat.setInputPaths(job, new Path(args[1] + "/temp"+(j-1)));
				  FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"+j));
			  }
			  
			  
			  job.setMapperClass(Map.class);
			  job.setCombinerClass(Reduce.class);
			  job.setReducerClass(Reduce.class);
			  job.setOutputKeyClass(Text.class);
			  job.setOutputValueClass(DoubleWritable.class);  
			  ControlledJob controlledJob = new ControlledJob(conf);
			  controlledJob.setJob(job);
			  if (j != 0){
				  controlledJob.addDependingJob(previousControlledJob);
			  }
			  jobControl.addJob(controlledJob);
			  previousControlledJob = controlledJob;
		  }
		  Thread jobControlThread = new Thread(jobControl);
		  jobControlThread.start();
		  
		  int timePassed = 0;
		  while (!jobControl.allFinished()) {
			    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
			    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
			    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
			    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
			    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
			    System.out.println("Time Passed: "+timePassed+"s");
			try {
			    Thread.sleep(30000);
			    timePassed += 30;
			    } catch (Exception e) {

			    }

			  } 
			   System.exit(0);  
			   return (job.waitForCompletion(true) ? 0 : 1);
  }

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> { //longwrite, text = input output = text, intWritable
    private final static IntWritable one = new IntWritable(1);
   // private Text word = new Text();
    private boolean caseSensitive = false;
   // private long numRecords = 0;
    private String input;
    private HashMap<String, String> nodeRelations = new HashMap<String, String>();
    //kan niet algemeen static gemaakt worden omdat het asynchroon is.
    
    
   // private Set<String> nodeRelations;
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*"); //spaces, tabs, and punctuation.

    protected void setup(Context context)
        throws IOException,
        InterruptedException {
      
    	if (context.getInputSplit() instanceof FileSplit) {
        this.input = ((FileSplit) context.getInputSplit()).getPath().toString();
      } else {
        this.input = context.getInputSplit().toString();
      }
      Configuration config = context.getConfiguration();
      
      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
      if (config.getBoolean("wordcount.skip.nodeRelations", false)) {
        URI[] localPaths = context.getCacheFiles();
        Set<String> lines = readFile(localPaths[0]);
        for (String line : lines){
        	String[] splittedLine = line.split(":");
        	if (splittedLine.length > 1){
    	    	nodeRelations.put(splittedLine[0], splittedLine[1]);
        	}
        	else{
        		//Er is sprake van een deadlink.
        		nodeRelations.put(splittedLine[0], "");
        	}
        }
      }
    }

	private Set<String> readFile(URI uri) {
		LOG.info("Added file to the distributed cache: " + uri);
		Set<String> lines = new HashSet<String>();
		  try {
		    BufferedReader reader = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));
		    String line;
		    while ((line = reader.readLine()) != null) {
		    	lines.add(line);
		    }
		    reader.close();
		  } catch (IOException ioe) {
		    System.err.println("Caught exception while parsing the cached file '"
		        + uri + "' : " + StringUtils.stringifyException(ioe));
		  }
		  return lines;
	}
	
    
    public void map(LongWritable offset, Text lineText, Context context) //offset, lineText van input af
        throws IOException, InterruptedException {
        //Ik ga er nu vanuit dat lineText de nodes.txt is.
    	String line = lineText.toString();
    	line = line.replace("\\s+", "");
    	
    	String[] words = line.split(":");
    	String node = words[0]; //also the key
		Double value = Double.parseDouble(words[1]);
		
		String nodeRelationsNode = nodeRelations.get(node);
    	if (!nodeRelationsNode.equals("")){ //Er is sprake van een deadlink als dit het geval is.
        	char[] relation = nodeRelationsNode.toCharArray();  	
        	double relationsLenght = relation.length;
        	double factor = 0.70 / relationsLenght; //factor krijgt afgeronde int terug tenzij relationsLenght een double is.
        	
        	List<Integer> aantalRelations = new ArrayList<Integer>();
        	for (char c : relation){
        		context.write(new Text(c+""), new DoubleWritable(value*factor));
        		//30% gaat naar de random jump chance, snap nog steeds niet waarom het random jump heet.
        		String cRelations = nodeRelations.get(c+"");
        		if (cRelations.equals("")){
        			aantalRelations.add(0);
        		}
        		else{
        			aantalRelations.add(cRelations.length());
        		}
        	}
        	int totalRelations = 0;
        	for (Integer i : aantalRelations){
        		totalRelations += i;
        	}
        	
        	double dTotalRelations = totalRelations;
        	int i = 0;
        	for (char c : relation){
        		double randomJumpFactor = (0.30 / dTotalRelations) * aantalRelations.get(i);
        		context.write(new Text(c+""), new DoubleWritable(value*randomJumpFactor));
        		i++;
        	}
        	
    	}   	
    	//0.3 random jump chance.
    	//random jump chance
    	//random naar uitgaande nodes. Hoe meer relaties uitgaande nodes hebben hoe meer ze krijgen, deadnodes krijgen niets.
    } //context.write gaat naar reduce <Test, intWritable,
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text node, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      for (DoubleWritable value : values) {
        sum += value.get();
      }
      //output has 2x :
      if(node.charAt(node.getLength() -1) == ':'){
    	  context.write(new Text(node), new DoubleWritable(sum));
      }
      else context.write(new Text(node+":"), new DoubleWritable(sum));
    }
  }
}