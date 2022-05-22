import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NB {

  public static int numFeatures = 16;

  public static int numColumns = numFeatures + 1;

  public static class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
      super(DoubleWritable.class);
    }
  }

  public static class NBMeanMapper
    extends Mapper<Object, Text, IntWritable, DoubleArrayWritable> {

    private Text word = new Text();

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");

      while (itr.hasMoreTokens()) {
        DoubleWritable[] temp = new DoubleWritable[numColumns];

        IntWritable klass = new IntWritable(Integer.parseInt(itr.nextToken()));

        for (int i = 0; i < numColumns - 1; i++) {
          temp[i] = new DoubleWritable(Double.parseDouble(itr.nextToken()));
        }

        temp[numColumns - 1] = new DoubleWritable(1);

        DoubleArrayWritable output = new DoubleArrayWritable();

        output.set(temp);

        context.write(klass, output);
      }
    }
  }

  public static class NBMeanCombiner
    extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {

    DoubleArrayWritable sum = new DoubleArrayWritable();

    public void reduce(
      IntWritable key,
      Iterable<DoubleArrayWritable> values,
      Context context
    )
      throws IOException, InterruptedException {
      DoubleWritable[] tempSum = new DoubleWritable[numColumns];

      for (int i = 0; i < numColumns; i++) {
        tempSum[i] = new DoubleWritable(0);
      }

      for (DoubleArrayWritable val : values) {
        String[] temp2 = val.toStrings();

        for (int i = 0; i < numColumns; i++) {
          tempSum[i] =
            new DoubleWritable(tempSum[i].get() + Double.parseDouble(temp2[i]));
        }
      }

      sum.set(tempSum);

      context.write(key, sum);
    }
  }

  public static class NBMeanReducer
    extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(
      IntWritable key,
      Iterable<DoubleArrayWritable> values,
      Context context
    )
      throws IOException, InterruptedException {
      DoubleWritable[] tempMean = new DoubleWritable[numFeatures];

      for (DoubleArrayWritable val : values) {
        String[] temp2 = val.toStrings();

        double sum = Double.parseDouble(temp2[numColumns - 1]);

        for (int i = 0; i < numFeatures; i++) {
          tempMean[i] = new DoubleWritable(Double.parseDouble(temp2[i]) / sum);
        }
      }

      DoubleArrayWritable mean = new DoubleArrayWritable();

      mean.set(tempMean);

      context.write(key, mean);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "NBMean");

    job.setJarByClass(NB.class);

    job.setMapperClass(NBMeanMapper.class);
    job.setCombinerClass(NBMeanCombiner.class);
    job.setReducerClass(NBMeanReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleArrayWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
