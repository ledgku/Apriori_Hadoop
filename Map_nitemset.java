package com.example.apriori;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Map_nitemset extends Mapper<Object, Text, Text, IntWritable>{
	private Text candidateText = new Text();
	private final static IntWritable ONE = new IntWritable(1);
	
	private List<String> lines = new ArrayList<String>();
	private List<String> candidateSet = new ArrayList<String>();
	
	@Override
	public void setup(Context context) throws IOException{
		String lastOutputFilePath = "/home/eduuser/Apriori/output"+(Integer.parseInt(context.getConfiguration().get("nth"))-1)+"/part-r-00000";
		Set<Integer> itemSet = new TreeSet<Integer>();
		
		Path path = new Path(lastOutputFilePath);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
		
		String line = null;
		
		while ((line = fis.readLine()) != null)
		{
			lines.add(line);
		}
		
		// i line
		for (int i=0; i<lines.size()-1; ++i){
			String[] iItems = lines.get(i).split("\\s+");
			
			itemSet.clear();
			
			for (int item = 0; item < iItems.length-1; ++item)
			{
				itemSet.add(Integer.parseInt(iItems[item]));
			}
			
			// i+x line
			for (int j=i+1; j<lines.size(); ++j){
				String[] jItems = lines.get(j).split("\\s+");
				
				for (int item = 0; item < jItems.length-1; ++item)
				{
					itemSet.add(Integer.parseInt(jItems[item]));
				}			
				
				if (itemSet.size() == Integer.parseInt(context.getConfiguration().get("nth")))
				{
					candidateSet.add(itemSet.toString());
				}			
				
				itemSet.clear();
				
				for (int item = 0; item < iItems.length-1; ++item)
				{
					itemSet.add(Integer.parseInt(iItems[item]));
				}
			}
		}
	}
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		HashSet<Integer> inputDataSet = new HashSet<Integer>();
		
		for (String src : value.toString().split("\\s+")){
			  inputDataSet.add(Integer.parseInt(src));
			}	
		
		for (int i=0; i<candidateSet.size(); ++i)
		{
			Pattern p = Pattern.compile("[\\[\\],]+");			
			Matcher m = p.matcher("");
			
			m.reset(candidateSet.get(i));
			String result = m.replaceAll("");
			
			String[] candidate = result.split("\\s+");
			boolean flag = true;
			
			for (String c : candidate){
				if (!inputDataSet.contains(Integer.parseInt(c))){
					flag = false;
					break;
				}
			}
		
			if (flag)
			{
				candidateText.set(result);
				context.write(candidateText, ONE);
			}
		}
	}
}
