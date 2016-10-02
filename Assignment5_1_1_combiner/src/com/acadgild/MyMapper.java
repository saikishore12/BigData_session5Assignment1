package com.acadgild;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
		String line=value.toString();
		String[] st=line.split("\n");
	
		for(int i=0;i<st.length;i++){
			String[] st1=st[i].split("\\|");			
			context.write(new Text(st1[0]), new IntWritable(1));
			}
	}
}