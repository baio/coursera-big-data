import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class WordsMapper extends Mapper<LongWritable, Text, Text, Text> {
		
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		
		//System.out.println(str);
		
		String [] strs = str.split(":::");
		
		if (strs.length == 3)
		{		
			String [] authors = strs[1].split("::");
			
			Text captionTxt = new Text(strs[2]);
					
			 for(int i = 0; i < authors.length ; i++)
			 {
			 	context.write(new Text(authors[i]), captionTxt);
			 }
		}
		 
		 //System.out.println("complete");
	}
}
