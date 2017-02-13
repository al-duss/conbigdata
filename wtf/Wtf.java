import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;
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

public class Wtf {

  /********************/
  /**    Mapper 1     **/
  /********************/

  public static class ReverseMapper 
  extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text values, Context context) 
    throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(values.toString());

      IntWritable follower = new IntWritable();
      IntWritable followed = new IntWritable();

      follower.set(Integer.parseInt(st.nextToken()));

      while(st.hasMoreTokens()) {
        followed.set(Integer.parseInt(st.nextToken()));
        context.write(followed, follower);
      }
    }
  }

  /**********************/
  /**      Reducer 1    **/
  /**********************/
  
  public static class ReverseReducer 
  extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      StringBuffer sb = new StringBuffer("");
      while(values.iterator().hasNext()) {
        int follower = values.iterator().next().get();
        sb.append(" "+follower);
      }
      context.write(key, new Text(sb.toString()));
    }

  }

  /********************/
  /**    Mapper 2     **/
  /********************/

  public static class AllPairsMapper 
  extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text values, Context context) 
    throws IOException, InterruptedException {

    }
  }

  /**********************/
  /**      Reducer 2    **/
  /**********************/
  
  public static class CountReducer 
  extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {

    }

  }


  public static void main(String[] args) 
  throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "who to follow");
    job.setJarByClass(Wtf.class);
    job.setMapperClass(ReverseMapper.class);
    job.setReducerClass(ReverseReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}