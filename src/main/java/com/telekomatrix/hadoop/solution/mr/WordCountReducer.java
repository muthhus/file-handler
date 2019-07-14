package com.telekomatrix.hadoop.solution.mr;


import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class WordCountReducer extends 
	 Reducer<Text, IntWritable, Text, ByteWritable> {

    public void reduce(Text key, 
                       Iterable<ByteWritable> values,
                       Context context) 
       throws IOException, InterruptedException {
       int sum = 0;
       for (ByteWritable val : values) {
          sum += val.get();
       }
       context.write(key, new ByteWritable()); 
    }
}
