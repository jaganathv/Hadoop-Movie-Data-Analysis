package Project1;

import java.io.IOException;

import java.util.Map;
import java.util.NavigableMap;

import java.util.TreeMap;
import Project1.IntPair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BestActorActressPair
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "BestActorActressPair");
	        job.setJarByClass(BestMovieByDecade.class); 
	 	    job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(BestActorPairMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntPair.class); 	    
	 	     	    
	 	    
		    job.setNumReduceTasks(1);
	 	    job.setReducerClass(BestActorPairReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
		    
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));    
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 	     	    	 	    
	}
}

class BestActorPairMapper extends Mapper<LongWritable, Text, Text, IntPair>
{
	 
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	 if(!line[4].equals("Actor") && !line[5].equals("Actress") && !line[7].equals("Popularity") && 
	     !line[5].equals("CAT") && !line[7].equals("CAT") && !line[7].equals("CAT") && 
		 !line[4].equals("") && !line[5].equals("") &&!line[7].equals("") &&
		 !line[8].equals("Awards") && !line[8].equals("BOOL") && !line[8].equals(""))
	    {
	
	       int Popularity = Integer.parseInt(line[7]);
		   String Awards = line[8].trim();
		   int Award_Ind = Integer.parseInt((Awards.equals("Yes"))?"0":"1");
		   
	       String Actor_Actress = line[4].trim()+";"+line[5].trim();
		       
	       context.write(new Text(Actor_Actress), new IntPair(new IntWritable(Popularity),new IntWritable(Award_Ind)));
	   
        }

  }
	 
  
} 

class BestActorPairReducer extends Reducer<Text,IntPair,Text,LongWritable> 
{
		
	//	Creating TreeMap to store Movie_Popularity by Actor_Name & Actress_Name
	     TreeMap<Long,String> countMap = new TreeMap<Long,String>();
	
    public void reduce(Text key, Iterable<IntPair> values,Context context) throws IOException, InterruptedException 
    {        
       int count=0;
       long sum_movie_popularity=0, avg_movie_popularity=0, movie_popularity=0;
	   int mov_pop=0,mov_awards=0; 
	           
       for (IntPair val:values)
       {      	
    	   mov_pop = val.getFirst().get();
    	   mov_awards = val.getSecond().get();
    	   if(mov_awards==1)
    	   {
    		   movie_popularity = Math.round(mov_pop + mov_pop*0.1);
    	   }
    	   else
    	   {
    		   movie_popularity = mov_pop;
    		   
    	   }
    	    		            
             sum_movie_popularity+=movie_popularity;
			 count+=1;
			 
	   }
             avg_movie_popularity = Math.round(sum_movie_popularity/count);
			 countMap.put(avg_movie_popularity,key.toString());
	   }   
	      
      
    
    public void cleanup(Context context) throws IOException, InterruptedException
    {         
		  
		     NavigableMap<Long, String> nMap = countMap.descendingMap();
		     
	        int count = 0;
			context.write(new Text("ACTOR-ACTRESS PAIR\t\tPOPULARITY"), new LongWritable(0));
    	for(Map.Entry<Long, String> entry : nMap.entrySet()) 
    	{
    		count ++;
    		if (count < 4)
    		{
    		  long popul_1 = entry.getKey();
    		  String actor_actress = entry.getValue();
			  context.write(new Text(actor_actress),new LongWritable(popul_1));
    		}
             else break;
                 		   
    	}																										
	}
}
	   
	
    