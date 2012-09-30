import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.*;


import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class WordsDriver extends Configured implements Tool {

		@Override
		public int run(String[] args) throws Exception {
							
			if (args.length != 2) {
				System.err.printf("Usage: %s [generic options] <input> <output>\n",
						getClass().getSimpleName());
				ToolRunner.printGenericCommandUsage(System.err);
				return -1;
			}
			
			Job job = new Job(getConf(), "words count");
			job.setJarByClass(WordsDriver.class);

			FileInputFormat.addInputPaths(job,args[0]);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
								
			job.setMapperClass(WordsMapper.class);
			//job.setCombinerClass(WordsReducer.class);
			job.setReducerClass(WordsReducer.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);


			/*
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			*/
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			System.exit(job.waitForCompletion(true) ? 0 : 1);

			return 0;
		}

		public static void main(String[] args) throws Exception {
			int exitCode = ToolRunner.run(new WordsDriver(), args);
			System.exit(exitCode);
		}
}

