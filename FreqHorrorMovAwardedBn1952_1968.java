package Project1;

import java.io.IOException;

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


public class FreqHorrorMovAwardedBn1952_1968
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "BestMovieByDecade");
	        job.setJarByClass(FreqHorrorMovAwardedBn1952_1968.class); 
	 	    job.setInputFormatClass(TextInputFormat.class);
			job.setMapperClass(FreqHorrorMovieMapper.class);
	 	    	    
		     
	 	     	    
	 	    
		    job.setNumReduceTasks(1);
	 	    job.setReducerClass(FreqHorrorMovieReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
		    
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));    
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 	     	    	 	    
	}
}

class FreqHorrorMovieMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	 
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	 if( !line[8].equals("Awards") && !line[0].equals("Year") &&
	    !line[8].equals("BOOL") && !line[0].equals("INT") &&
	    !line[8].equals("") && !line[0].equals(""))
	 {
	
	    String Awards = line[8].trim();
	    int Year = Integer.parseInt(line[0]);
     
	 if (Year>=1952 && Year<=1968)
	    { 	 
          context.write(new Text(Awards), new IntWritable(1));
		}
	  
	   
     }

  }
	 
  
} 

class FreqHorrorMovieReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	String delim2 =";";
	String header1 = "HORROR_MOVIE", header2="COUNT";

	public void setup(Context context) throws IOException, InterruptedException 
	{
		context.write(new Text(header1+"\t\t"+header2), new IntWritable(1));
	}
	 
	
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {        
       
       int sum=0;
       for (IntWritable val:values)
       {      	
            sum+=val.get();
	   }   
	       context.write(new Text(key), new IntWritable(sum));
	  
    }  
    
			

	       
     }

    
    
 
	   
	
    