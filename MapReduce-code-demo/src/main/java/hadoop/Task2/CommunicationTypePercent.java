package hadoop.Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Date;

public class CommunicationTypePercent {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long startTime=new Date().getTime();
        if (args.length != 2) {
            System.err
                    .println("Usage: CommunicationTypePercent <input path> <output path>");
            System.exit(-1);
        }
//        String input = "C:\\Users\\Lenovo\\Desktop\\MapReduce-code-demo\\input\\testdata.txt";
//        String output = "C:\\Users\\Lenovo\\Desktop\\MapReduce-code-demo\\outputtest2";

        //1、设置job的基础属性
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(CommunicationTypePercent.class);
        job.setJobName("CommunicationTypePercent");

        //2、设置Map与Reudce的类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //3、设置map与reduce的输出键值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //4、设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //5、提交执行
        System.out.println("start");
        job.waitForCompletion(true);
        long endTime = new Date().getTime();
        System.out.println("本程序运行 " + (endTime - startTime)
                + " 毫秒完成。" );
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {


            String line = value.toString();
            String[] srcData = line.split("\t");

            String calling_optr = srcData[3];
            String called_optr=srcData[4];
            String call_type = srcData[12];

            context.write(new Text(call_type + calling_optr), new IntWritable(1));
            context.write(new Text(call_type + called_optr), new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum = sum + value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
