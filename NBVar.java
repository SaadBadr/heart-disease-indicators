import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

public class NBVar {

  public static int numFeatures = 16;
  public static int numColumns = numFeatures + 1;
  public static int varLength = (numFeatures * (numFeatures + 1)) / 2;

  public static class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
      super(DoubleWritable.class);
    }
  }

  public static class NBVarMapper
    extends Mapper<Object, Text, IntWritable, DoubleArrayWritable> {

    List<String> findDecimalNums(String stringToSearch) {
      Pattern decimalNumPattern = Pattern.compile("-?\\d+(\\.\\d+)?");
      Matcher matcher = decimalNumPattern.matcher(stringToSearch);

      List<String> decimalNumList = new ArrayList<>();
      while (matcher.find()) {
        decimalNumList.add(matcher.group());
      }

      return decimalNumList;
    }

    private Text word = new Text();
    Double[] meanX = new Double[numFeatures];
    Double[] meanY = new Double[numFeatures];

    @Override
    protected void setup(Context context)
      throws IOException, InterruptedException {
      Path pt = new Path("/NBTut/Output2/params.txt");
      FileSystem fs = FileSystem.get(context.getConfiguration());

      BufferedReader br = new BufferedReader(
        new InputStreamReader(fs.open(pt))
      );

      try {
        String line;
        line = br.readLine();
        while (line != null) {
          List<String> s = findDecimalNums(line);
          DoubleWritable[] temp = new DoubleWritable[numFeatures];
          for (int i = 1; i < numColumns; i++) {
            meanX[i - 1] = Double.parseDouble(s.get(i));
          }

          line = br.readLine();
          s = findDecimalNums(line);
          temp = new DoubleWritable[numFeatures];
          for (int i = 1; i < numColumns; i++) {
            meanY[i - 1] = Double.parseDouble(s.get(i));
          }
          line = br.readLine();
        }
      } finally {
        // you should close out the BufferedReader
        br.close();
      }
    }

    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), ",");

      while (itr.hasMoreTokens()) {
        Double[] temp = new Double[numColumns];
        IntWritable klass = new IntWritable(Integer.parseInt(itr.nextToken()));
        if (klass.get() == 0) {
          for (int i = 0; i < numFeatures; i++) {
            temp[i] = Double.parseDouble(itr.nextToken()) - meanX[i];
          }
        } else {
          for (int i = 0; i < numFeatures; i++) {
            temp[i] = Double.parseDouble(itr.nextToken()) - meanY[i];
          }
        }

        DoubleWritable[] temp2 = new DoubleWritable[varLength + 1];
        int k = 0;
        for (int i = 0; i < numFeatures; i++) {
          for (int j = i; j < numFeatures; j++) {
            temp2[k] = new DoubleWritable(temp[i] * temp[j]);
            k++;
          }
        }

        temp2[varLength] = new DoubleWritable(1);
        DoubleArrayWritable output = new DoubleArrayWritable();
        output.set(temp2);
        context.write(klass, output);
      }
    }
  }

  public static class NBVarCombiner
    extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {

    DoubleArrayWritable sum = new DoubleArrayWritable();

    public void reduce(
      IntWritable key,
      Iterable<DoubleArrayWritable> values,
      Context context
    )
      throws IOException, InterruptedException {
      DoubleWritable[] tempSum = new DoubleWritable[varLength + 1];
      for (int i = 0; i < varLength + 1; i++) {
        tempSum[i] = new DoubleWritable(0);
      }

      for (DoubleArrayWritable val : values) {
        String[] temp2 = val.toStrings();
        for (int i = 0; i < varLength + 1; i++) {
          tempSum[i] =
            new DoubleWritable(tempSum[i].get() + Double.parseDouble(temp2[i]));
        }
      }
      sum.set(tempSum);

      context.write(key, sum);
    }
  }

  public static class NBVarReducer
    extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(
      IntWritable key,
      Iterable<DoubleArrayWritable> values,
      Context context
    )
      throws IOException, InterruptedException {
      DoubleWritable[] tempVariance = new DoubleWritable[varLength + 1];

      for (DoubleArrayWritable val : values) {
        String[] temp2 = val.toStrings();
        double sum = Double.parseDouble(temp2[varLength]);
        for (int i = 0; i < varLength; i++) {
          tempVariance[i] =
            new DoubleWritable(Double.parseDouble(temp2[i]) / sum);
        }
        tempVariance[varLength] =
          new DoubleWritable(Double.parseDouble(temp2[varLength]));
      }

      DoubleArrayWritable variance = new DoubleArrayWritable();
      variance.set(tempVariance);

      context.write(key, variance);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job varJob = Job.getInstance(conf, "NBVar");

    varJob.setJarByClass(NBVar.class);
    varJob.setMapperClass(NBVarMapper.class);
    varJob.setCombinerClass(NBVarCombiner.class);
    varJob.setReducerClass(NBVarReducer.class);

    varJob.setOutputKeyClass(IntWritable.class);
    varJob.setOutputValueClass(DoubleArrayWritable.class);

    FileInputFormat.addInputPath(varJob, new Path(args[0]));
    FileOutputFormat.setOutputPath(varJob, new Path(args[1]));

    System.exit(varJob.waitForCompletion(true) ? 0 : 1);
  }
}
