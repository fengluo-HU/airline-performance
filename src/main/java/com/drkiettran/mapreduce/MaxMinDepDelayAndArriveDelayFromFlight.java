package com.drkiettran.mapreduce;

import java.io.IOException;

import com.drkiettran.mapreduce.helper.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVParser;

/**
 * Let's see if we could wordcount to work. This is a classic program that is
 * used for concept of mapreduce programming.
 * 
 * https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 * 
 */
public class MaxMinDepDelayAndArriveDelayFromFlight {

    public static class maxminmapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
        Text t1 = new Text();
    
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() > 0) {
                String[] lines = new CSVParser().parseLine(value.toString());
                //use combination of UniqueCarrier and FlightNum to identifier flights
                t1.set(lines[8] + lines[9] + " departure delay"); // the 8th index is that for the UniqueCarrier of airline 
                //the 9th index is that for the FlightNum of airline
                //handle NA parse error
                context.write(t1, new IntWritable(IntergerConverter.parseWithDefault(lines[15], 0))); // the 15th index is that for the departure delay of airline
                t1.set(lines[8] + lines[9] + " arrive delay");                                                       // departure delay
                context.write(t1, new IntWritable(IntergerConverter.parseWithDefault(lines[14], 0))); // the 14th index is that for the arrive delay of airline
                                                                        // arrive delay
            }
        } 
    } 
    
    public static class maxminReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    
        int min = Integer.MAX_VALUE, max = 0;

        for (IntWritable value : values) {
            int v = value.get();
            if (v < min) { //Finding min value
        
                min = v;
            
            }
            
            if (v > max) { //Finding max value
        
            max = v;
        
            } 
        }
        
        context.write(new Text(key), new IntWritable(min));

        context.write(new Text(key), new IntWritable(max));
    
        } 
    }
    
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find Minimum and Maximum for departureDelay and arrive delay for flights");
		job.setJarByClass(MaxMinDepDelayAndArriveDelayFromFlight.class);

        job.setOutputKeyClass(Text.class);        
        job.setOutputValueClass(IntWritable.class);       
        job.setMapperClass(maxminmapper.class);    
        job.setCombinerClass(maxminReducer.class);    
        job.setReducerClass(maxminReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int result = job.waitForCompletion(true) ? 0 : 1;
		//printResult(args[1]);
		System.exit(result);         
    }
}
