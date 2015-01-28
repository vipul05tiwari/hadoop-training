/*
 * The information contained in this document is subject to change without notice.
 * 
 * Developer MAKES NO WARRANTY OF ANY KIND WITH REGARD TO
 * THIS MATERIAL, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Except to
 * correct same after receipt of reasonable notice, GoldenSource Corporation 
 * shall not be liable for errors contained herein or for incidental and/or 
 * consequential damages in connection with the furnishing, performance, 
 * or use of this material.
 * 
 * This document contains proprietary and confidential information that is protected by copyright.
 * 
 * The names of other organizations and products referenced herein are the trademarks or service
 * marks (as applicable) of their respective owners. Unless otherwise stated herein, no association
 * with any other organization or product referenced herein is intended or should be inferred.
 * 
 * 
 */

package com.vipul.training.hadoop.mapred.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * {@link WordCountMapper} is mapper class for word count program.
 *
 * @author vipul
 * @see 
 * @Date 26-Jan-2015
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	@Override
	public void map(LongWritable key, Text value,  Context context) throws IOException, InterruptedException 
	{
		 String line = value.toString();
		 StringTokenizer tokenizer = new StringTokenizer(line);
	       while (tokenizer.hasMoreTokens())
	       {
		       word.set(tokenizer.nextToken());
		       context.write(word, one);
		   }
     }
}
