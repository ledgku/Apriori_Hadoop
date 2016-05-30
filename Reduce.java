package com.example.apriori;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int countItemFreq = 0;
		
		for (IntWritable value : values){
			countItemFreq += value.get();
		}
		
		int minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
		
		if (countItemFreq >= minsup)
		{
			context.write(key, new IntWritable(countItemFreq));
		}
	}
}
