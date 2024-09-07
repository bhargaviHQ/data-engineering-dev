package com.bd.longestword.driver.test;
  
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver; 
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.bd.longestword.mapper.LongestWordMapper;
import com.bd.longestword.reducer.LongestWordReducer;

public class TestLongestWordDriver {
	MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

	@Before
	public void setup() {
		LongestWordReducer reducer = new LongestWordReducer();
		LongestWordMapper map = new LongestWordMapper(); 
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(map,reducer);
	}
 
	@Test
	public void testDriver() {
		mapReduceDriver.withInput(new LongWritable(), new Text("ca^t smal*l ##d#og")); 
		mapReduceDriver.withOutput(new IntWritable(5), new Text("small"));  
		try {
			mapReduceDriver.runTest();
		} catch (IOException ioexception) { 
			ioexception.printStackTrace();
		}
	}

}
