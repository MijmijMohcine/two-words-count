
package org.hadoop.wordcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import java.io.IOException;



public class WCountReduce extends Reducer<Text, IntWritable, Text, Text>
{
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		// To loop through all values ​​associated with the provided key.
		Iterator<IntWritable> i=values.iterator();
		int count=0;
		while(i.hasNext())   // For each value
			count+=i.next().get();    // ...we add it to the total.
		// We return the pair (key; value) made up of our key and the total, in Text format.
		context.write(key, new Text("\t"+count));
  }
}
