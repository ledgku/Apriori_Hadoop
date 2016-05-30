package com.example.apriori;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Apriori extends Configured  implements Tool{

	public static void main(String[] args) throws Exception {
		if (args.length != 4)
		{
			System.err.println("Usage : <input file path> <output dir> <minsup> <max itemset size>");
			System.exit(1);
		}
		
		int res = ToolRunner.run(new Configuration(), new Apriori(), args);
		
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {		
		String hdfsInputFile = args[0];
		String hdfsOutputDir = args[1];
		
		Integer MIN_SUPPORT_PERCENT = Integer.parseInt(args[2]);
		Integer MAX_ITEMSET_SIZE = Integer.parseInt(args[3]);
		
		System.out.println("Input File :\t" + hdfsInputFile);
		System.out.println("Output Dir :\t" + hdfsOutputDir);
		System.out.println("Minsup :\t" + MIN_SUPPORT_PERCENT);
		System.out.println("Max Itemset Size :\t" + MAX_ITEMSET_SIZE);
		
		long startTime = System.currentTimeMillis();
		long endTime = System.currentTimeMillis();
		
		for (int size=1; size <= MAX_ITEMSET_SIZE; ++size)
		{
			boolean continueFlag = runAprioriJob(hdfsInputFile, hdfsOutputDir, size, MIN_SUPPORT_PERCENT);
			
			endTime = System.currentTimeMillis();
			System.out.println(size+"-itemset :\t" + (endTime - startTime));
			
			if (!continueFlag)
			{
				System.out.println("Maximun itemset size :\t" + size);
				break;
			}
		}
		
		endTime = System.currentTimeMillis();
		System.out.println("Total time taken :\t" + (endTime - startTime));
		
		return 0;
	}

	private boolean runAprioriJob(String hdfsInputFile, String hdfsOutputDir, Integer nth, Integer minsup) throws IOException, ClassNotFoundException, InterruptedException
	{	
		Job job = Job.getInstance(getConf());
		job.getConfiguration().set("minsup", Integer.toString(minsup));
		job.getConfiguration().set("nth", Integer.toString(nth));
		job.setJarByClass(Apriori.class);
		
		if (nth == 1) {
		  job.setMapperClass(Map_1itemset.class);
		} 
		else {
		  job.setMapperClass(Map_nitemset.class);
		}
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	 
		FileInputFormat.addInputPath(job, new Path(hdfsInputFile));
		FileOutputFormat.setOutputPath(job, new Path(hdfsOutputDir + nth));
		
		return job.waitForCompletion(true) ? true : false; 
	}
}
