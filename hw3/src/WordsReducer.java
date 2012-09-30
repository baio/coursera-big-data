import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WordsReducer extends Reducer<Text, Text, Text, Text> {

	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		Map<String, Integer> map = new HashMap<String, Integer>();
						
		for (final Text line : values) {
			
			String str = line.toString();
			
			str = str.replace('(',' ').replace(')',' ').replace('.',' ').replace(',',' ').replace('-', ' ');
			
			String [] words = str.split(" ");
						
			for(int i = 0; i < words.length - 1; i++)
			{
				String word = words[i];
				
				if (word != null && !word.isEmpty() && word.length() != 1)
				{
					word = word.toLowerCase();
																			
					if (CheckStopWords(word))
					{						
						Integer cnt = 0;
						
						if (map.containsKey(word))
						{
							cnt = map.get(word);							
						}
												
						map.put(word, cnt + 1);								
					}														
				}							
			}			
		}
		
		StringBuilder sb = new StringBuilder();
			    
	    for (Entry<String, Integer> entry : map.entrySet()) {
	        	    		   
	        sb.append("," + entry.getKey() + ":" + entry.getValue());	        
	    }
							 	
	 	context.write(key, new Text(sb.toString()));	
				
	}
	
	private boolean CheckStopWords(String Word)
	{
		for (String str : StopWords.List)
		{
			if (str == Word) return false;
		}
		
		return true;		
	}
}
