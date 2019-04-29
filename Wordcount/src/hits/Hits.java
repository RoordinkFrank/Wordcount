package hits;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

public class Hits extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(Hits.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Hits(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
	      JobControl jobControl = new JobControl("jobChain");
	      System.out.println("vlak voor de nullpointers");
	      ControlledJob previousControlledJob = null;
	      Job job = null;
	      System.out.println("vlak na de nullpointers");
		  URI skipUri = null;
		  URI incomingUri = null;
		  int lastStep = 4;
		  for (int stepNumber = 0;stepNumber<lastStep;stepNumber++){ //first step autority, 2nd hub, 3rth normalise, 4th start again
			  Configuration conf = getConf();
			  System.out.println("Current step: "+stepNumber);
			  
			  job = Job.getInstance(conf, "stap"+stepNumber);
			  
			  
			  if(stepNumber % 3 == 0){
				  job.getConfiguration().setBoolean("wordcount.step.changeAuthority", true);
			  }
			  else if (stepNumber % 3 == 1){
				  job.getConfiguration().setBoolean("wordcount.step.changeHub", true);
				  
			  }
			  else if (stepNumber % 3 == 2){
				  job.getConfiguration().setBoolean("wordcount.step.normalise", true);
			  }
			  
			  
			  if(stepNumber == 0){
				  System.out.println("argsLength: "+args.length);
				  for (int i = 0; i < args.length; i += 1) {
					  System.out.println();
				      if ("-skip".equals(args[i])) {
				        job.getConfiguration().setBoolean("wordcount.skip.nodeOutgoingRelations", true);
				        i += 1;
				        skipUri = new Path(args[i]).toUri();
				        job.addCacheFile(skipUri);
				        
				        // this demonstrates logging
				        LOG.info("Added file to the distributed cache: " + args[i]);
						System.out.println("skipuri succesfull aangemaakt");
				      }
				      else if ("incoming".equals(args[i])){
				    	  job.getConfiguration().setBoolean("wordcount.incoming.nodeIncomingRelations", true);
					        i += 1;
					        incomingUri = new Path(args[i]).toUri();
					        job.addCacheFile(incomingUri);
					        
					        // this demonstrates logging
					        LOG.info("Added file to the distributed cache: " + args[i]);
							System.out.println("incomingUri succesfull aangemaakt");
				      }
				    }
			  }
			  else{
				  if(skipUri != null && incomingUri != null){
					    if (job.getConfiguration().getBoolean("wordcount.step.changeAuthority", false)) {
							  job.getConfiguration().setBoolean("wordcount.skip.nodeOutgoingRelations", true);
							  job.addCacheFile(skipUri);
							  System.out.println("skipuri succesfull overgenomen");
							  //volgensmij gaat dit goed in asynchroon, ook als je niet meerekend dat alle stappen dependend op elkaar zijn.
					    }
					    else if (job.getConfiguration().getBoolean("wordcount.step.changeHub", false)) {
					    	job.getConfiguration().setBoolean("wordcount.incoming.nodeIncomingRelations", true);
							job.addCacheFile(incomingUri);
						    System.out.println("incomingUri succesfull overgenomen");
					    }

				  }
			  }
			  
			  job.setJarByClass(this.getClass());
			  if (stepNumber == 0){
				  FileInputFormat.setInputPaths(job, new Path(args[0]));
				  FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp0"));
			  }
			  else if (stepNumber == (lastStep - 1)){
				  FileInputFormat.setInputPaths(job, new Path(args[1] + "/temp"+(stepNumber-1)));
				  FileOutputFormat.setOutputPath(job, new Path(args[1] + "/final"));
			  }
			  else{
				  FileInputFormat.setInputPaths(job, new Path(args[1] + "/temp"+(stepNumber-1)));
				  FileOutputFormat.setOutputPath(job, new Path(args[1] + "/temp"+stepNumber));
			  }
			  
			  
			  job.setMapperClass(Map.class);
			  job.setCombinerClass(Reduce.class);
			  job.setReducerClass(Reduce.class);
			  job.setOutputKeyClass(Text.class);
			  job.setOutputValueClass(DoubleWritable.class);  
			  ControlledJob controlledJob = new ControlledJob(conf);
			  controlledJob.setJob(job);
			  if (stepNumber != 0){
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
    private String input;
    private HashMap<String, String> nodeRelations = new HashMap<String, String>();
    //private HashMap<String, String> nodeIsKnownBy = new HashMap<String, String>();
    //kan niet algemeen static gemaakt worden omdat het asynchroon is.
    
    
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
      
      if (config.getBoolean("wordcount.skip.nodeOutgoingRelations", false) || config.getBoolean("wordcount.incoming.nodeIncomingRelations", false)) {
        URI[] localPaths = context.getCacheFiles();
        Set<String> lines = readFile(localPaths[0]); //localpath, dit gaat mogelijk fout, wordcount voorbeeld deed het echter ook.
        //het is niet supermoeilijk dit stukje te verplaatsen mochten meerdere comps in het spel komen en dit een probleem blijken.
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
      
      /*
      //hoeft alleen tijdens hub berekening, doet wel nu alles omdat de node pas in de mapper zelf bekend wordt. Kan misch nog efficienter.
      //Dit zou elke mapper hoet hoeven te berekenen. Echter hij maakt gebruik van nodeKNows en dat gebeurt hier ook al.
      if(config.getBoolean("wordcount.step.changeHub", false)){
    	  for (Entry<String, String> relation : nodeRelations.entrySet()){
    		  for(char c : relation.getValue().toCharArray()){
    			  String oldValue = nodeIsKnownBy.get(c);
    			  if (oldValue == null){
    				  nodeIsKnownBy.put(c+"", relation.getKey());
    			  }
    			  else {
    				  nodeIsKnownBy.put(c+"", oldValue+relation.getKey());
    			  }		  
    		  }
    	  }
      }*/
      
      //calculate normalisationvalues (has to be done one time for all mappers)
      //I think this setup means that it's still done everymapper though
      
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
		Double value1 = Double.parseDouble(words[1]); //value die straks overschreden wordt. stap0 node:aut:hub, stap1 node:hub:aut stap2(norma) node:aut:hub
		Double value2 = Double.parseDouble(words[2]);
		
		    //increase authority
        	if (context.getConfiguration().getBoolean("wordcount.step.changeAuthority", false)){
            	writeToReducer(context, value2, nodeRelations, node); //nodeOutgoingRelations
        	}
        	//increase hub
        	else if (context.getConfiguration().getBoolean("wordcount.step.changeHub", false)){		
            	writeToReducer(context, value2, nodeRelations, node); //nodeIncomingRelations
        	}
        	//normalise (reducer only get's one value)
        	else if (context.getConfiguration().getBoolean("wordcount.step.normalisation", false)){
        		
        	}
    	}

	private void writeToReducer(
			Mapper<LongWritable, Text, Text, DoubleWritable>.Context context,
			Double value2, HashMap<String, String> hashMap, String node) throws IOException, InterruptedException {
		String relations = hashMap.get(node);
    	if (!relations.equals("")){ //Er is sprake van een deadlink als dit het geval is.
    		//output has 2x :
    	    if(node.charAt(node.length() -1) == ':'){
    	    	for (char c : relations.toCharArray()){
        			context.write(new Text(c+""+value2), new DoubleWritable(value2));
        		}
    	    }
    	    else {
    	    	for (char c : relations.toCharArray()){
        			context.write(new Text(c+":"+value2), new DoubleWritable(value2));
        		}
    	    }	
    	}
	}
    } //context.write gaat naar reduce <Test, intWritable,
  

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    public void reduce(Text node, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0;
      for (DoubleWritable value : values) {
        sum += value.get();
      }
      //output has 2x : if i don't use this. No idea why.
      if(node.charAt(node.getLength() -1) == ':'){
    	  context.write(new Text(node), new DoubleWritable(sum));
      }
      else context.write(new Text(node+":"), new DoubleWritable(sum));
    }
  }
}