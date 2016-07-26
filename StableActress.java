package Project1;

import java.io.IOException;
import java.util.Set;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

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

public class StableActress
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "StableActress");
	        job.setJarByClass(StableActress.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(StableActressMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(DoublePair.class);
		    job.setMapOutputValueClass(TextPair.class);
		    job.setSortComparatorClass(Top5StableSortComp.class);
		    job.setGroupingComparatorClass(Top5StableGroupComp.class);
            	    
 		    job.setNumReduceTasks(1);
 		    job.setPartitionerClass(Top5StableJoinPart.class);
	 	    job.setReducerClass(StableActressReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class StableActressMapper extends Mapper<LongWritable, Text, DoublePair, TextPair>
{
	Multimap<String, Integer> multiMap = ArrayListMultimap.create();
	
	
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
		 
	 if(!line[5].equals("Actress") && !line[7].equals("Popularity") && 
	     !line[5].equals("CAT") && !line[7].equals("INT") &&
	     !line[5].equals("") && !line[7].equals("")) 
	    {
		   
	       int Popularity = Integer.parseInt(line[7]);
		   String Actress = line[5].trim();
	
		   multiMap.put(Actress, Popularity);		       		   
   
        }
  }	 
   public void cleanup(Context context) throws IOException, InterruptedException
   {	
	Set<String> keys = multiMap.keySet(); 
	
	
		for (String key : keys) 
		  {
	
	                    
						 ArrayList<Integer> pop= new ArrayList<Integer>();
                           						 
	                     Iterator<Integer>  c = multiMap.get(key).iterator();
	
	
	             	      while(c.hasNext())
	           	           {		              	
	
						      pop.add(c.next());
		            	   
	        	            }
	              	             
		
		 Double pop_mean = computeMean(pop);
		 Double pop_variance = computeVariance(pop); 
		 Double pop_sd =  Math.sqrt(pop_variance);
		 // Clearing all elements from the ArrayList before the next call
		 pop.clear();
		
		String pop_sd_as_string = pop_sd.toString();
		
		
		context.write(new DoublePair(new DoubleWritable(pop_mean), new DoubleWritable(pop_variance)), new TextPair(new Text(pop_sd_as_string), new Text(key)));
		  }
	   
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

class StableActressReducer extends Reducer <DoublePair,TextPair,Text,Text> 
{
		
	//	Creating LinkedHashMap to populate Actor Name, Mean, Standard Deviation and Variance
	     LinkedHashMap<String, String> hMap = new LinkedHashMap<String, String>();
	     int task = 0; 
	
    public void reduce(DoublePair key, Iterable<TextPair> values,Context context) throws IOException, InterruptedException 
    {        
         String pop_actress="", actress_std="", mean_var_std="";
		 
       for (TextPair val:values)
       {      	
    	 
	       pop_actress = val.getSecond().toString();		   
    	   actress_std = val.getFirst().toString();
		   mean_var_std = key.getFirst().get() +"\t"+ key.getSecond().get()+"\t"+ actress_std;
		 
		   hMap.put(pop_actress,mean_var_std);  	   
			 
	   }
       
  }   	      
      
    
	public void cleanup(Context context) throws IOException, InterruptedException
    {         
		  
		  
		  Iterator<String> i=hMap.keySet().iterator();
		  int count=0;
		  context.write(new Text("Actress Name"), new Text("Popularity Variance Stdev"));
		  while(i.hasNext() && ++count<6)
		  {  
			String final_key = i.next();  
		
			context.write(new Text(final_key), new Text(hMap.get(final_key)));
			
		  }
		     																									
	}
}
	   
	
    