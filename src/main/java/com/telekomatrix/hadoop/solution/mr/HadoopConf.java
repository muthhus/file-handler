package com.telekomatrix.hadoop.solution.mr;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="hadoop")
public class HadoopConf {

	private String input;
	private String output;
	private String url;


	
	public String getInput() {
		return input;
	}



	public void setInput(String input) {
		this.input = input;
	}



	public String getOutput() {
		return output;
	}



	public void setOutput(String output) {
		this.output = output;
	}



	public String getUrl() {
		return url;
	}



	public void setUrl(String url) {
		this.url = url;
	}



	public org.apache.hadoop.conf.Configuration getLocalHadoopConf() throws Exception {

		// ====== Init HDFS File System Object
		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		// Set FileSystem URI
		conf.set("fs.defaultFS", url);
		// Because of Maven
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// Set HADOOP user
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		System.setProperty("hadoop.home.dir", "/");
		//Get the filesystem - HDFS
		FileSystem fs = FileSystem.get(URI.create(url), conf);
		return conf;
	}
	

	
	
	
}
