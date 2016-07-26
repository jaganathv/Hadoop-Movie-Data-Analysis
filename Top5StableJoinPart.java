package Project1;



import org.apache.hadoop.mapreduce.Partitioner;

public class Top5StableJoinPart extends Partitioner<DoublePair, TextPair> {
	  
    @Override
    public int getPartition(DoublePair key, TextPair val, int numPartitions) 
    {
    	Double val1 = key.getFirst().get();
       
    	int hash1 = val1.hashCode() & Integer.MAX_VALUE;
    	       		   	
        int partition = hash1 % numPartitions;
        return partition;
    }
 
}


