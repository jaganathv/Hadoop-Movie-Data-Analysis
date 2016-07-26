package Project1;

import java.io.IOException;
import java.util.Set;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

//import java.util.HashMap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
//import java.util.Map;
//import java.util.Map;
//import java.util.Map.Entry;//

import Project1.DoublePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StableActor
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "StableActor");
	        job.setJarByClass(StableActor.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(StableActorMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(DoublePair.class);
		    job.setMapOutputValueClass(TextPair.class);
		    job.setSortComparatorClass(Top5StableSortComp.class);
		    job.setGroupingComparatorClass(Top5StableGroupComp.class);
            	    
 		    job.setNumReduceTasks(1);
 		    job.setPartitionerClass(Top5StableJoinPart.class);
	 	    job.setReducerClass(StableActorReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class StableActorMapper extends Mapper<LongWritable, Text, DoublePair, TextPair>
{
	Multimap<String, Integer> multiMap = ArrayListMultimap.create();
	
	
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	// System.out.println("Actor:"+ line[4].trim()+"\t"+"Popularity:"+Integer.parseInt(line[7].trim()));
	 
	 if(!line[4].equals("Actor") && !line[7].equals("Popularity") && 
	     !line[4].equals("CAT") && !line[7].equals("INT") &&
	     !line[4].equals("") && !line[7].equals("")) 
	    {
		   
	       int Popularity = Integer.parseInt(line[7]);
		   String Actor = line[4].trim();
		  // System.out.println("Actor:"+Actor+"\t"+"Popularity:"+Popularity);
		   multiMap.put(Actor, Popularity);		       		   
   
        }
  }	 
   public void cleanup(Context context) throws IOException, InterruptedException
   {	
	Set<String> keys = multiMap.keySet(); 
	//System.out.println("keys.size():"+keys.size());
			//System.out.println("Popularity for each movie by actor");
	
		for (String key : keys) 
		  {
	                   // System.out.println("Key = " + key);
	                    
						 ArrayList<Integer> pop= new ArrayList<Integer>();
                           						 
	                     Iterator<Integer>  c = multiMap.get(key).iterator();
	                    // int no_of_mov=0;
	
	             	      while(c.hasNext())
	           	           {
			              	
	             	    	  //System.out.println(c.next());
						      pop.add(c.next());
						     // ++no_of_mov;
	            	   
	        	            }
	              	             
		// System.out.println(key+"\t"+no_of_mov);
		// no_of_mov=0;
		 Double pop_mean = computeMean(pop);
		 Double pop_variance = computeVariance(pop); 
		 Double pop_sd =  Math.sqrt(pop_variance);
		 // Clearing all elements from the ArrayList before the next call
		 pop.clear();
		
		String pop_sd_as_string = pop_sd.toString();
		//System.out.println(key+"\t"+pop_mean+"\t"+pop_variance+"\t"+pop_sd);
		
		
		context.write(new DoublePair(new DoubleWritable(pop_mean), new DoubleWritable(pop_variance)), new TextPair(new Text(pop_sd_as_string), new Text(key)));
		  }
	   //System.out.println("MOVING TO REDUCER CODE");
	   }
		public double computeMean(ArrayList<Integer> popularity)
		{
 		   int pop_total=0;	
		  for(int i=0;i<popularity.size();i++)
   		    {
		       pop_total+=popularity.get(i);			
		    }
		   return (pop_total/popularity.size());		
		}

		public double computeVariance(ArrayList<Integer> popularity)
		{
		  if (popularity.size()==0)   return Double.NaN;
		  double avg = computeMean(popularity);
		  double sum=0.0;
		  for(int i=0;i<popularity.size();i++)
		   {
		     sum+= (popularity.get(i)-avg) *  (popularity.get(i)-avg);
		   }
		   return (sum/popularity.size());					
		}

				
  
} 

class StableActorReducer extends Reducer <DoublePair,TextPair,Text,Text> 
{
		
	//	Creating HashMap to populate Actor Name, Mean, Standard Deviation and Variance
	     LinkedHashMap<String, String> hMap = new LinkedHashMap<String, String>();
	     int task = 0; 
	
    public void reduce(DoublePair key, Iterable<TextPair> values,Context context) throws IOException, InterruptedException 
    {        
         String pop_actor="", act_std="", mean_var_std="";
		 //System.out.println("\nTASK #"+ ++task);
		 //System.out.println("KEY CD:"+ key);
		 //System.out.print("VALUES:");
       for (TextPair val:values)
       {      	
    	   // System.out.println(" "+val.getFirst().toString());
	       pop_actor = val.getSecond().toString();		   
    	   act_std = val.getFirst().toString();
		   mean_var_std = key.getFirst().get() +"\t"+ key.getSecond().get()+"\t"+ act_std;
		  // System.out.println("POPULATED VALUE IN HMAP");
		   //System.out.println(pop_actor+" "+mean_var_std);		   
		   hMap.put(pop_actor,mean_var_std);  	   
			 
	   }
       
  }   	      
      
    
	public void cleanup(Context context) throws IOException, InterruptedException
    {         
		  
		  //Set<Entry<String, String>> set=hMap.entrySet();
		//System.out.println("IN CLEANUP METHOD OF REDUCER:");
		//System.out.println("ACTOR"+" "+"MEAN_VARIANCE_STD");
		
		  
		  Iterator<String> i=hMap.keySet().iterator();
		  int count=0;
		  context.write(new Text("Actor Name"), new Text("Popularity Variance Stdev"));
		  while(i.hasNext() && ++count<6)
		  {  
			String final_key = i.next();  
		//	System.out.println(final_key+" "+hMap.get(final_key));
			context.write(new Text(final_key), new Text(hMap.get(final_key)));
			
		  }
		     																									
	}
}
	   
	
    