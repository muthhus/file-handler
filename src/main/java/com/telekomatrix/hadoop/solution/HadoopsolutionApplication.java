package com.telekomatrix.hadoop.solution;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.telekomatrix.hadoop.solution.files.SmallFileHandler;
import com.telekomatrix.hadoop.solution.mr.HadoopConf;

@SpringBootApplication
@EnableConfigurationProperties(HadoopConf.class)
public class HadoopsolutionApplication implements CommandLineRunner {
	
	@Autowired
	private HadoopConf hadoopConf;

	public static void main(String[] args) {
		SpringApplication.run(HadoopsolutionApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		SmallFileHandler sfHandler = new SmallFileHandler(hadoopConf);
		sfHandler.combinedSmallFiles();
		
	}

}
