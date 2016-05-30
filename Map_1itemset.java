package com.example.apriori;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Map_1itemset extends Mapper<Object, Text, Text, IntWritable>{
	private Text item = new Text();
	private final static IntWritable ONE = new IntWritable(1);
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		for (String src : value.toString().split("\\s+")){
		  item.set(src);
		  context.write(item, ONE);
		}		
	}
}
