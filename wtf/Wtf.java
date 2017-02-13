import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Iterator;
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
  /**    Mapper 1    **/
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
  /**      Reducer 1   **/
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
  /**    Mapper 2    **/
  /********************/

  public static class AllPairsMapper 
  extends Mapper<Object, Text, IntWritable, IntWritable> {

    public void map(Object key, Text values, Context context) 
    throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(values.toString());

      ArrayList<Integer> sameFollowers = new ArrayList<>();

      IntWritable follower1 = new IntWritable();
      IntWritable follower2 = new IntWritable();
      IntWritable followed = new IntWritable();
      followed.set(Integer.parseInt(st.nextToken()));

      while(st.hasMoreTokens()) {
        follower1.set(Integer.parseInt(st.nextToken()));
        for (Integer sameFollow : sameFollowers){
          follower2.set(sameFollow);
          context.write(follower1, follower2);
          context.write(follower2, follower1);
        }
        sameFollowers.add(follower1.get());
      }
      //Have a negative value for all followers on the followed person.
      for(Integer follower : sameFollowers) {
        follower1.set(-follower);
        context.write(followed, follower1);
      }
    }
  }

  /**********************/
  /**      Reducer 2   **/
  /**********************/
  
  public static class FollowReducer 
  extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
    
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
    throws IOException, InterruptedException {
      IntWritable follower = new IntWritable();
      TreeMap<Integer, Integer> map = new TreeMap<>();
      while(values.iterator().hasNext()) {
        int recommended = values.iterator().next().get();
        Integer count = map.get(recommended);
        if (count == null) {
          map.put(recommended,1);
        } else {
          map.put(recommended,count+1);
        }
      }
      removeAlreadyFollowing(map);
      for(Map.Entry<Integer,Integer> entry: map.entrySet()) {
        follower.set(entry.getKey());
        StringBuffer sb = new StringBuffer("");
        sb.append(" "+key.toString()+"("+entry.getValue()+")");
        context.write(follower, new Text(sb.toString()));
      }
    }
  }

  public static void removeAlreadyFollowing(TreeMap<Integer, Integer> map){
    for(Map.Entry<Integer,Integer> entry : new TreeMap<Integer,Integer>(map).entrySet()) {
      //TreeMap already sorts, therefore can exit loop once positive
      if(entry.getKey() > 0){
        break;
      }
      //Put a 0 on entries that need to be removed
      map.put(-entry.getKey(),0);
    }
    //Remove entries that are negative or have a value of 0
    for(Iterator<Map.Entry<Integer, Integer>> it = map.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<Integer,Integer> entry = it.next();
      if(entry.getKey() < 0 || entry.getValue() == 0){
        it.remove();
      }
    }
  }

  public static void main(String[] args) 
  throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "who to follow");
    job1.setJarByClass(Wtf.class);
    job1.setMapperClass(ReverseMapper.class);
    job1.setReducerClass(ReverseReducer.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp"));
    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "who to follow2");
    job2.setJarByClass(Wtf.class);
    job2.setMapperClass(AllPairsMapper.class);
    job2.setReducerClass(FollowReducer.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path("temp"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }

}