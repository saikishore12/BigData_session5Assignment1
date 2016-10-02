package com.acadgild.extend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {



		for (IntWritable units : values) {

			context.write(key,units);
	}
		
		//context.write(key, new IntWritable(NoOfUnits));
	}
}
