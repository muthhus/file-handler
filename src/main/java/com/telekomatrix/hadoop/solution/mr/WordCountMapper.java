package com.telekomatrix.hadoop.solution.mr;

import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.commons.lang.StringUtils;

import edu.umd.cloud9.io.pair.PairOfStringLong;

public class WordCountMapper extends 
	 Mapper<PairOfStringLong, Text, Text, ByteWritable> {

    final static ByteWritable one = new ByteWritable();
    private Text word = new Text();

    public void map(PairOfStringLong key, 
                    Text value,
                    Context context) 
       throws IOException, InterruptedException {
       String line = value.toString().trim();
       String[] tokens = StringUtils.split(line, " ");
       for (String tok : tokens) {
          word.set(tok);
          context.write(word, one);  
       }
    }
}
