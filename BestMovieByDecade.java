package Project1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import Project1.TextPair;

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

import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

public class BestMovieByDecade
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "BestMovieByDecade");
	        job.setJarByClass(BestMovieByDecade.class); 
	 	    job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(movieMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(TextPair.class); 	    
	 	     	    
	 	    
		    job.setNumReduceTasks(9);
	 	    job.setReducerClass(movieReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
		    
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));    
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 	     	    	 	    
	}
}

class movieMapper extends Mapper<LongWritable, Text, IntWritable, TextPair>
{
	 
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	 if((!line[2].equals("Title") && !line[7].equals("Popularity") && !line[0].equals("Year")) &&
	   (!line[2].equals("STRING") && !line[7].equals("CAT") && !line[0].equals("INT")) &&
	   (!line[2].equals("") && !line[7].equals("") && !line[0].equals("")))
	 {
	
	    String Movie_Popularity_Year = line[2]+";"+line[7]+";"+line[0];
	    int Year = Integer.parseInt(line[0]);
     
	 if (Year>=1920 && Year<=1929) 
	 { context.write(new IntWritable(1),new TextPair(new Text(Movie_Popularity_Year),new Text("1920-1929"))); }
	else if (Year>=1930 && Year<=1939) 
	{ context.write(new IntWritable(2),new TextPair(new Text(Movie_Popularity_Year),new Text("1930-1939")));}
	else if (Year>=1940 && Year<=1949) 
	{ context.write(new IntWritable(3),new TextPair(new Text(Movie_Popularity_Year),new Text("1940-1949")));}
	else if (Year>=1950 && Year<=1959) 
	{ context.write(new IntWritable(4),new TextPair(new Text(Movie_Popularity_Year),new Text("1950-1959")));}
	else if (Year>=1960 && Year<=1969) 
	{ context.write(new IntWritable(5),new TextPair(new Text(Movie_Popularity_Year),new Text("1960-1969")));}
	else if (Year>=1970 && Year<=1979) 
	{ context.write(new IntWritable(6),new TextPair(new Text(Movie_Popularity_Year),new Text("1970-1979")));}
	else if (Year>=1980 && Year<=1989) 
	{ context.write(new IntWritable(7),new TextPair(new Text(Movie_Popularity_Year),new Text("1980-1989")));}
	else if (Year>=1990 && Year<=1999) 
	{ context.write(new IntWritable(8),new TextPair(new Text(Movie_Popularity_Year),new Text("1990-1999")));}
	else { context.write(new IntWritable(9),new TextPair(new Text(Movie_Popularity_Year),new Text("2000 or greator")));}
	  
	   
  }

  }
	 
  
} 

class movieReducer extends Reducer<IntWritable,TextPair,Text,Text> 
{
	String delim2 =";";
	String header1 = "MOVIE_YEAR", header3="POPULARITY",header2="";

	//	Creating SortedSetMultiMap to store values in reverse order
	SortedSetMultimap<Integer,String> sortmulMap=TreeMultimap.create(Ordering.natural().reverse(), Ordering.natural());
	
    public void reduce(IntWritable key, Iterable<TextPair> values,Context context) throws IOException, InterruptedException 
    {        
       String[] value1 = new String[100]; 
       String value2="";
       int value1_pop=0;
       
       for (TextPair val:values)
       {      	
           // Storing the Movie, Popularity & Year in Value1 field
           value1 = val.getFirst().toString().split(delim2);
           value1_pop=Integer.parseInt(value1[1]);
           
		   
		   // Storing the Year Range in Value2 field
		   value2 = val.getSecond().toString();
		   
		   // Writing the Popularity as key and (Movie & Year) values in value in sortedMap		   
		   sortmulMap.put(value1_pop,value1[0]+"\t"+value1[2]);
	   }   
	       // Creating Headers for Output File
		   header2=value2+"\t";
	  
    }  
    
    public void cleanup(Context context) throws IOException, InterruptedException
    {         
		  // Set<Entry<Integer, String>> s= sortedMap.entrySet();
		     Set<Integer> keys = sortmulMap.keySet();
		     
	        int loop=0;	
			for (Integer key : keys) 
		      {
				loop+=1;
				Iterator<String>  c = sortmulMap.get(key).iterator();
				while(c.hasNext())
	               {
					 String op_value = c.next().toString();																																																																																																																																																														
				     context.write(new Text(key.toString()),new Text(op_value));
	               }
	                    
				if(loop==1)
				{
					break;
				}
	          }																																																																																																																																																																																																																																																																																									
			

	       
     }

    
    
}
	   
	
    