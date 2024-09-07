package com.bd.longestword.mapper.test;
 
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver; 
import org.junit.Before;
import org.junit.Test;

import com.bd.longestword.mapper.LongestWordMapper; 

public class TestLongestWordMapper {
	 MapDriver<LongWritable, Text, IntWritable, Text> mapDriver; 

	 @Before
	 public void setup(){
		 LongestWordMapper map = new LongestWordMapper();
			mapDriver  = MapDriver.newMapDriver(map);
	 }
			  
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(),new Text("cat!! !apple $beautiful @old t+eams")); 
		mapDriver.withOutput(new IntWritable(3), new Text("old"));
		mapDriver.withOutput(new IntWritable(5), new Text("teams")); 
		mapDriver.withOutput(new IntWritable(9), new Text("beautiful")); 
		try {
			mapDriver.runTest();
		} catch (IOException ioexception) { 
			ioexception.printStackTrace();
		}
	}
	
	@Test(expected=NullPointerException.class)
	public void testMapperNullPointerException(){
		String nullString = null ; 
		mapDriver.withInput(new LongWritable(),new Text(nullString)); 
		try {
			mapDriver.runTest();
		} catch (IOException ioexception) { 
			ioexception.printStackTrace();
		}
	}

}
