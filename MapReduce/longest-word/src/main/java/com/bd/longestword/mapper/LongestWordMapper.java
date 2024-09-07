package com.bd.longestword.mapper;

import java.io.IOException;
import java.util.HashMap; 
import java.util.Map; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;    
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Bhargavi Sriram
 * Mapper class to find the longest word . 
 * THe purpose of the class is to produce a map task.
 * The map takes input and then splits it into key value pairs
 *
 */
public class LongestWordMapper extends
Mapper<LongWritable, Text, IntWritable, Text> {

	private static Logger LOGGER = LoggerFactory.getLogger(LongestWordMapper.class); 

	public HashMap<IntWritable, Text> valueStore = new HashMap<IntWritable, Text>(); 

	public void setup() {

	}

	@Override
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
		LOGGER.info("Map function executing....."); 
		String line = value.toString();

		String[] words = line.split(" ");

		for (String word : words) { 

			word = word.replaceAll("[-+.^:,!@#$%~*]",""); 

			Text outputKey = new Text(word);
			valueStore.put(new IntWritable(outputKey.toString().length()),
					outputKey);
			LOGGER.info("String: "+outputKey.toString().length() );
			LOGGER.info("String: "+outputKey);
		}

		LOGGER.info("Map output : ");

		for (Map.Entry<IntWritable, Text> pair : valueStore.entrySet()) {
			try {
				con.write(pair.getKey(), pair.getValue());
				LOGGER.info(pair.getKey().toString() + pair.getValue().toString());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

	public void cleanup(Context con) { 

	}
}
