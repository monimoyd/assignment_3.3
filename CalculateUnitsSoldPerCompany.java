
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CalculateUnitsSoldPerCompany 
{
	public static class CalculateUnitsSoldPerCompanyMapper
	    extends Mapper<Object, Text, Text,IntWritable>{

	      private Text word = new Text();
	      private final static IntWritable one = new IntWritable(1);

	 	  public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	 		  String record = value.toString();
	 		  if (record.length() > 0) {	      
	 			     StringTokenizer st = new StringTokenizer(record, "|");
	 			     String company= st.nextToken();
	 			     String product= st.nextToken();
	 			     
	 			    if ("NA".equals(company) || "NA".equals(product)) {
	 			    	// Skip the Record as it is invalid
	 			    	
	 			    } else {			     
	                  word.set(company);
	                  context.write(word, one);
	 			    }
	              
	          }

	      }

	  }
	
	public static class CalculateUnitsSoldPerCompanyReducer   
	       extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
        
            result.set(sum);
            context.write(key, result);

         }

    }

	   
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Calculate Units Sold Per Company");
	    job.setJarByClass(CalculateUnitsSoldPerCompany.class);

	    job.setMapperClass(CalculateUnitsSoldPerCompanyMapper.class);		  
	    job.setCombinerClass(CalculateUnitsSoldPerCompanyReducer.class);
	    job.setReducerClass(CalculateUnitsSoldPerCompanyReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


