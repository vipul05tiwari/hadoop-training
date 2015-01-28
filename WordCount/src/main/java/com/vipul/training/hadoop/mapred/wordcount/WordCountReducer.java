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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * {@link WordCountReducer} is a reducer program for word count.
 *
 * @author vipul
 * @see 
 * @Date 26-Jan-2015
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	@Override
	public void reduce(Text text, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		int sum = 0;
		for (IntWritable value : values) 
		{
            sum += value.get();
        }
        context.write(text, new IntWritable(sum));
	}
}
