package com.telekomatrix.hadoop.solution.mr;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import edu.umd.cloud9.io.pair.PairOfStringLong;

import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;


public class CustomInputFileFormat extends CombineFileInputFormat<PairOfStringLong, Text> {

	final static long MAX_SPLIT_SIZE_64MB = 67108864; // 64 MB = 64*1024*1024
	   
	   public CustomInputFileFormat() {
	      super();
	      setMaxSplitSize(MAX_SPLIT_SIZE_64MB); 
	   }
	  
	   public RecordReader<PairOfStringLong, Text> createRecordReader(InputSplit split, 
	                                                                  TaskAttemptContext context) 
	      throws IOException {
	      return new CombineFileRecordReader<PairOfStringLong, Text>((CombineFileSplit)split, 
	                                                                 context, 
	                                                                 CustomRecordReader.class);
	   }
	  
	   @Override
	   protected boolean isSplitable(JobContext context, Path file) {
	      return false;
	   }

}
