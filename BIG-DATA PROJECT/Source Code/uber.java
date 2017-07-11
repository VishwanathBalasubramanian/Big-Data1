package uber_proj.uber_proj;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class uber 
{
    public static class Map extends Mapper<Object, Text, Text, Text>
    {
    	//private final static IntWritable one = new IntWritable(1);
    	private Text basement_date = new Text();	
    	private Text basement_id = new Text();
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
           String line = value.toString();
           String[] splits = line.split(",");
           if(splits.length >=3)
           {
        	   if(Integer.parseInt(splits[2]) > 1000)
               {
        		   basement_id.set(splits[0]);
        		   basement_date.set("|"+splits[1]+"|"+splits[2]);
 	   
               	}
           
            context.write(basement_id,basement_date);
           }
    }
		public Object keySet() {
			// TODO Auto-generated method stub
			return null;
		}
 
    }
 
    
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "uber");
         
        job.setJarByClass(uber.class); 
        job.setMapperClass(Map.class);
       
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    }