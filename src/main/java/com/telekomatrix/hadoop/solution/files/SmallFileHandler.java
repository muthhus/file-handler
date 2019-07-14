package com.telekomatrix.hadoop.solution.files;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import com.telekomatrix.hadoop.solution.files.handlers.FilesHandlerUtils;
import com.telekomatrix.hadoop.solution.mr.CustomInputFileFormat;
import com.telekomatrix.hadoop.solution.mr.HadoopConf;
import com.telekomatrix.hadoop.solution.mr.WordCountMapper;
import com.telekomatrix.hadoop.solution.mr.WordCountReducer;

@Component
public class SmallFileHandler extends Configured {

	private HadoopConf hadoopConf;
	
	@Autowired
	public SmallFileHandler(HadoopConf hadoopConf) {
		this.hadoopConf = hadoopConf;
	}
	
	
	public void combinedSmallFiles() throws Exception {
		 System.out.println("input path = "+ hadoopConf.getInput());
	     System.out.println("output path = "+ hadoopConf.getOutput());
//	     System.out.println("Config path =" + hadoopConf.getLocalHadoopConf());
	     
	     Configuration conf = hadoopConf.getLocalHadoopConf();//getConf();
	     Job job = new Job(conf);
	     job.setJobName("SmallFileHandler");
	     
	     // add jars in hdfs:///lib/*.jar to Hadoop's DistributedCache
	     // we place all our jars into HDFS's /lib/ directory
	     FilesHandlerUtils.addJarsToDistributedCache(job, "/lib/");     
	    
	     // define your custom plugin class for input format
	     job.setInputFormatClass(CustomInputFileFormat.class);
	    
	     // define mapper's output (K,V)
	     job.setMapOutputKeyClass(Text.class);
	     job.setMapOutputValueClass(ByteWritable.class);
	    
	     // define map() and reduce() functions
	     job.setMapperClass(WordCountMapper.class);
	     job.setReducerClass(WordCountReducer.class);
	     //job.setNumReduceTasks(13);
	    
	     // define I/O
	     Path inputPath = new Path(hadoopConf.getInput());
	     Path outputPath = new Path(hadoopConf.getOutput());
	     FileInputFormat.addInputPath(job, inputPath);
	     FileOutputFormat.setOutputPath(job, outputPath);
	    
	     // submit job and wait for its completion
	     job.submit();
	     job.waitForCompletion(true);

	}

}
