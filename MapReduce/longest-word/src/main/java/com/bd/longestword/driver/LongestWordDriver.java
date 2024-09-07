package com.bd.longestword.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bd.longestword.mapper.LongestWordMapper;
import com.bd.longestword.reducer.LongestWordReducer;

/**
 * 
 * @author Bhargavi Sriram
 * 
 *         The purpose of the class is to create a job and pass the request to
 *         map and reduce task. The application aims to find the longest word
 *         from the input file given as argument.
 *
 */
public class LongestWordDriver {

	private static Logger LOGGER = LoggerFactory
			.getLogger(LongestWordDriver.class);

	@SuppressWarnings("deprecation")
	public static void main(String[] args)

	{

		if (args.length == 2) { 
			
			Configuration c = new Configuration(); 
			 
			Path output = new Path(args[1]);

			Job job;
			try {

				job = new Job(c, "longestword");

				job.setJarByClass(LongestWordDriver.class);

				job.setMapperClass(LongestWordMapper.class);

				job.setReducerClass(LongestWordReducer.class);

				job.setCombinerClass(LongestWordReducer.class);

				job.setSortComparatorClass(Comparator.class);

				job.setOutputKeyClass(IntWritable.class);

				job.setOutputValueClass(Text.class);

				LOGGER.info("Driver class function");

				/*
				 * Getting the input files from the folder.
				 */
				FileSystem fs = FileSystem.get(c);
				FileStatus[] status_list = fs.listStatus(new Path(args[0]));
				if (status_list != null) {
					for (FileStatus status : status_list) {
			/**
			 * Add input path through MultipleInputs
			 */
						MultipleInputs.addInputPath(job, status.getPath(),
								TextInputFormat.class, LongestWordMapper.class); 
					}
				}

				FileOutputFormat.setOutputPath(job, output);
				try {
					System.exit(job.waitForCompletion(true) ? 0 : 1);
				} catch (ClassNotFoundException classNotFoundException) {
					classNotFoundException.printStackTrace();
				} catch (InterruptedException interruptedException) {
					interruptedException.printStackTrace();
				}
			} catch (IOException ioexception) {
				ioexception.printStackTrace();
			}
		} else {
			System.err
					.println("Number of arguements do not match, Thank you !" + args.length);
		}

	}

}