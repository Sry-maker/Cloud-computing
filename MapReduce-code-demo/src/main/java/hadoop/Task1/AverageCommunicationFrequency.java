package hadoop.Task1;

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

public class AverageCommunicationFrequency {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long startTime=new Date().getTime();
        if (args.length != 2) {
            System.err
                    .println("Usage: AverageCommunicationFrequency <input path> <output path>");
            System.exit(-1);
        }
//        String input = "C:\\Users\\Lenovo\\Desktop\\MapReduce-code-demo\\input\\testdata.txt";
//        String output = "C:\\Users\\Lenovo\\Desktop\\MapReduce-code-demo\\outputtest1";

        //1、设置job的基础属性
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(AverageCommunicationFrequency.class);
        job.setJobName("AverageCommunicationFrequency");

        //2、设置Map与Reudce的类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //3、设置map与reduce的输出键值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

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

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

//            通过空格分割字符串
            String line = value.toString();
            String[] srcData = line.split("\t");

            String day_id = srcData[0];
            String calling_nbr = srcData[1];
            String called_nbr = srcData[2];
            String start_time = srcData[9];

            context.write(new Text(calling_nbr), new Text(calling_nbr + ' ' + day_id + ' ' + start_time));
            context.write(new Text(called_nbr), new Text(called_nbr + ' ' + day_id + ' ' + start_time));
//            System.out.println("map");
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

//            平均通话次数=总通话次数/天数(参考原始数据中为29天)
            int allcall = 0;
            for (Text value : values) {
                allcall++;
            }

            double averagecall = allcall / 29.0;
            context.write(key, new DoubleWritable(averagecall));
        }
    }
}
