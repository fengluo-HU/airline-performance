package com.drkiettran.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;

/*
 * Extracted from Hadoop for Dummies (2014)
 */
public class FlightsOriginatedFromAirpoirtMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() > 0) {
			String[] lines = new CSVParser().parseLine(value.toString());
			context.write(new Text(lines[16]), new IntWritable(1)); // the 16th index is that for the origin of airline
																	// origin
		}
	}
}