package com.acadgild.extend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import com.acadgild.MyMapper;
import com.acadgild.MyReducer;

public class Driver1 {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "driver");		
		job1.setJarByClass(Driver1.class);
		job1.setMapperClass(MyMapper.class);
		job1.setReducerClass(MyReducer.class);
		job1.setCombinerClass(MyReducer.class);
	
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outputPath=new Path("mapreduce1output");
		FileOutputFormat.setOutputPath(job1,outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		job1.waitForCompletion(true);
	
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "driver1");		
		job2.setJarByClass(Driver1.class);
		
		job2.setReducerClass(MyReducer.class);
		//job2.setCombinerClass(MyReducer.class);
	
		//job2.setMapOutputKeyClass(Text.class);
		//job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, outputPath);
		FileOutputFormat.setOutputPath(job2,new Path(args[1]));
		//outputPath.getFileSystem(conf).delete(outputPath);
		job2.waitForCompletion(true);
		

	}

}
