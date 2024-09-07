package com.bd.longestword.reducer.test;
 
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test; 

import com.bd.longestword.reducer.LongestWordReducer;

public class TestLongestWordReducer {
	 ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
	 
	 @Before
	 public void setup(){
		 LongestWordReducer reducer = new LongestWordReducer();
		 reduceDriver = ReduceDriver.newReduceDriver(reducer);
	 }
	@Test
	public void testReducer() { 
		 List<Text> values = new ArrayList<Text>();
		    values.add(new Text("beautiful")); 
		    values.add(new Text("beopsiful"));
		    reduceDriver.withInput(new IntWritable(9),values );
		    reduceDriver.withOutput(new IntWritable(9), new Text("beautiful")); 
		    try {
		    	reduceDriver.runTest();
			} catch (IOException ioexception) { 
				ioexception.printStackTrace();
			}
	}
	 
	@Test(expected = NullPointerException.class)
	public void testNullList() { 
		String value = null ; 
		List<Text> values = new ArrayList<Text>();
		    values.add(new Text(value)); 
		    reduceDriver.withInput(new IntWritable(9),values );  
		    try {
		    	reduceDriver.runTest();
			} catch (IOException ioexception) { 
				ioexception.printStackTrace();
			}
	}
}
