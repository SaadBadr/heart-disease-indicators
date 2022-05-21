import java.io.IOException;

import java.util.StringTokenizer;



import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class NB {



	public static class DoubleArrayWritable extends ArrayWritable {

		public DoubleArrayWritable() { 

			super(DoubleWritable.class); 

			}

	}



	public static class NBMapper

       extends Mapper<Object, Text, IntWritable, DoubleArrayWritable>{



		private Text word = new Text();

		int numColumns = 17;



		public void map(Object key, Text value, Context context

						) throws IOException, InterruptedException {

			

			StringTokenizer itr = new StringTokenizer(value.toString(), ",");

			

			while (itr.hasMoreTokens()) {



				DoubleWritable[] temp = new DoubleWritable[numColumns];

				IntWritable one = new IntWritable(1);

				for(int i = 0 ; i < numColumns; i++){

					temp[i] = new DoubleWritable(Double.parseDouble(itr.nextToken()));

				}

				

				DoubleArrayWritable output = new DoubleArrayWritable();

				output.set(temp);

				context.write(one, output);

				// 1 [40 5 2 1 2]

			}

		}

  }



  public static class NBCombiner

       extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {



		int numColumns = 17;



		DoubleArrayWritable sum = new DoubleArrayWritable();

		int sum_keys = 0;

		

		public void reduce(IntWritable key, Iterable<DoubleArrayWritable> values,

						Context context

						) throws IOException, InterruptedException {

			DoubleWritable[] temp = new DoubleWritable[numColumns];	

				for(int i = 0 ; i < numColumns; i++){

					temp[i] = new DoubleWritable(0);

				}

				

			for (DoubleArrayWritable val : values) {

				String[] temp2 = val.toStrings();

				for(int i = 0 ; i < numColumns; i++){

					temp[i] = new DoubleWritable(temp[i].get() + Double.parseDouble(temp2[i]));

				}

				sum.set(temp);

				sum_keys += 1;

			}

			

			IntWritable summation = new IntWritable(sum_keys);

			context.write(summation, sum);

		}

  }



  public static class NBReducer

       extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {

    private IntWritable result = new IntWritable();

		int numColumns = 17;



    public void reduce(IntWritable key, Iterable<DoubleArrayWritable> values,

                       Context context

                       ) throws IOException, InterruptedException {



	  DoubleWritable[] temp = new DoubleWritable[numColumns];	

	  

	  for (DoubleArrayWritable val : values) {

	  	String[] temp2 = val.toStrings();

	  	for(int i = 0 ; i < numColumns; i++){

	  		temp[i] = new DoubleWritable(Double.parseDouble(temp2[i]) / key.get());

	  	}

	  }

	  DoubleArrayWritable sum = new DoubleArrayWritable();

	  sum.set(temp);

	  

      context.write(key, sum);

    }

  }



  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "NB");



    job.setJarByClass(NB.class);

    job.setMapperClass(NBMapper.class);

    job.setCombinerClass(NBCombiner.class);

    job.setReducerClass(NBReducer.class);



    job.setOutputKeyClass(IntWritable.class);

    job.setOutputValueClass(DoubleArrayWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}