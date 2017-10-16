package fr.ece.Pivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.tools.javac.util.List;


/**
 * Hello world!
 *
 */
public class Pivot 
{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{

	     private Text word = new Text();
	
	     @Override
	     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	 	{
	    	IntWritable outputKey = new IntWritable(0);
	    	int nb = 0;
	    	
	 		String line = value.toString();
	 		String[] elements=line.split(",");
	 		for(String element: elements )
	 		{
	 		    word = new Text(element.trim());
	 		    
	 		  	nb = outputKey.get();
	 		  	nb++;
	 		  	outputKey.set(nb);
	 		  	
	 		  	//System.out.println(outputKey);
	 		  	//System.out.println(word);
	 		  	context.write(outputKey, word);
	 		}
	 	}
    }


    public static class Reduce extends Reducer<IntWritable,Text,IntWritable,Text> {
     //private Text result = new Text();

     @Override
	     public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	   Text result = new Text();
    	   String resultString = new String();
    	   
	       //System.out.println(key.get());
	  	   for(Text value : values)
	  	   {
	  	   		result.set(result + value.toString() +",");
	  	   	//System.out.println(value.toString());
	  	   }
	  	   resultString = result.toString();
	  	   resultString = resultString.substring(0, resultString.length() - 1);
	  	   result.set(resultString);
	  	   
	  	   context.write(key, result);
	     }
    }

    public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
     Job job = Job.getInstance(conf, "pivot");
     job.setJarByClass(Pivot.class);
     job.setMapperClass(Map.class);
     job.setCombinerClass(Reduce.class);
     job.setReducerClass(Reduce.class);
     job.setOutputKeyClass(IntWritable.class);
     job.setOutputValueClass(Text.class);
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    }