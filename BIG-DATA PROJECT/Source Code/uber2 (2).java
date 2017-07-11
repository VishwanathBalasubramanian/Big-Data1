package uber_proj.uber_proj;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import uber_proj.uber_proj.sort_uti; 

public class uber2 
{
    public static class TripsMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
    	private DoubleWritable trips = new DoubleWritable();	
    	private Text basement_id = new Text();
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException 
        {
           String line = value.toString();
           String[] splits = line.split(",");
           if(splits.length >=3)
           {
        	   basement_id.set(new Text(splits[0]));
        	   trips.set(Double.parseDouble(splits[3]));
        		}           
            context.write(basement_id,trips);
           }
    }
 
    
    public static class AvgReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> 
    
    {
        private DoubleWritable result = new DoubleWritable();
        private Map<String,Double> countmap= new HashMap<String,Double>();
        public void reduce(Text key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException 
        {
            int sum = 0;
            int count = 0 ;
            for (DoubleWritable val : values) 
            {
                sum += val.get();
                count++;
            }
            double avg = (double) sum/count;
            result.set(avg);
            countmap.put(key.toString(), avg);
          //  context.write(key, result);
        }

    protected void cleanup(Context context) throws IOException, InterruptedException {

		HashMap<String,Double> sortedMap = sort_uti.sortByValues(countmap);

        int counter = 0;
        for (Map.Entry<String,Double> x : sortedMap.entrySet()) {
        	counter ++;  	
            if (counter == 4){
                break;
            }
            context.write(new Text(x.getKey()),new DoubleWritable(x.getValue()));         
        }
    }


    } 
	

	public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "uber2");
         
        job.setJarByClass(uber2.class); 
        job.setMapperClass(TripsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(AvgReducer.class);
         
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}