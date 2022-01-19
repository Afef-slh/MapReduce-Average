package tn.isima.average;

import java.io.IOException;





import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Reduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

int sum = 0;
int count =0;

public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    
	
    for(IntWritable x : values){
    	sum+= x.get();
    	count ++;
        }
    int average = sum/count;
    System.out.print("le somme de likes est: "+ average );
    context.write(NullWritable.get(), new IntWritable(average));
    	
    }
    

}