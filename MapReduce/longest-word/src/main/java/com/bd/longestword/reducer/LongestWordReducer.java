package com.bd.longestword.reducer;

import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * 
 * 
 * @author Bhargavi Sriram
 * 
 * The reduce class is to reduce the input from the 
 * map task , and produce output 
 * output is written to the file in the driver
 *
 */
public class LongestWordReducer extends
		Reducer<IntWritable, Text, IntWritable, Text>

{
	private static Logger LOGGER = LoggerFactory
			.getLogger(LongestWordReducer.class);

	private int maxLength = 0;
	private String longestWord = "";

	public void reduce(IntWritable length, Iterable<Text> word, Context context)
			throws IOException, InterruptedException {
		LOGGER.info("Reducer funbction");
		if (maxLength < length.get()) {
			maxLength = length.get();
			longestWord = word.iterator().next().toString();
		}
	}

	public void cleanup(Context context) throws IOException,
			InterruptedException {
		LOGGER.info("Reducer output :");
		context.write(new IntWritable(maxLength), new Text(longestWord));
		LOGGER.info(maxLength + longestWord);

	}
}
