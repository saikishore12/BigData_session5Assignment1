package com.acadgild;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partitioner1 extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable values, int numPartitions) {

		String st = key.toString();
		
		if (st.substring(0, 1).matches("[A-F]")) {
			return 0;
		} else if (st.substring(0, 1).matches("[G-M]")) {
			return 1;
		} else if (st.substring(0, 1).matches("[N-R]")) {
			return 2;
		} else {
			return 3;
		}
		
	}

}