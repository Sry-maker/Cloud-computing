package hadoop.Task3;

import hadoop.Task1.AverageCommunicationFrequency;
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
import java.util.Arrays;
import java.util.Date;

public class CommunicationTimePercent {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        long startTime = new Date().getTime();
        if (args.length != 2) {
            System.err
                    .println("Usage: CommunicationTimePercent <input path> <output path>");
            System.exit(-1);
        }
//        String input = "C:\\Users\\Lenovo\\Desktop\\MapReduce-code-demo\\input\\testdata.txt";
//        String output = "C:\\Users\\Lenovo\\Desktop\\MapReduce-code-demo\\outputtest10";

        //1、设置job的基础属性
        Configuration configuration = new Configuration();
        Job job = new Job(configuration);
        job.setJarByClass(CommunicationTimePercent.class);
        job.setJobName("CommunicationTimePercent");

        //2、设置Map与Reudce的类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //3、设置map与reduce的输出键值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
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

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] srcData = line.split("\t");
            String calling_nbr = srcData[1];
            String called_nbr = srcData[2];
            String start_time = srcData[9];
            String raw_dur = srcData[11];

            //通话开始时间
            String[] startTime = start_time.split(":");
            int startHour = Integer.parseInt(startTime[0]);
            int startMinute = Integer.parseInt(startTime[1]);
            int startSecond = Integer.parseInt(startTime[2]);

            //设置边界及时间段时间
            int[] boundary = {3, 6, 9, 12, 15, 18, 21, 24};
            int[] timePeriod = {0, 0, 0, 0, 0, 0, 0, 0};

            int period = startHour / 3;
            int remainTime = Integer.parseInt(raw_dur);
            int boundaryremain = 3600 * (boundary[period] - startHour) - 60 * startMinute - startSecond;

            //剩余时间小于剩余边界
            if (remainTime <= boundaryremain) {
                timePeriod[period] = remainTime;
            }
            //剩余时间大于剩余边界
            if (remainTime > boundaryremain) {
                timePeriod[period] = boundaryremain;
                remainTime = remainTime - boundaryremain;
                int addperioid = remainTime / (3600 * 3);
                for (int i = 0; i < addperioid; i++) {
                    timePeriod[(period + addperioid) % 8] = 3600 * 3;
                }
                int addremianTime = remainTime % (3600 * 3);
                if (addremianTime != 0) {
                    timePeriod[(period + addperioid + 1) % 8] = addremianTime;
                }
            }


            String str = timePeriod[0] + "\t" + timePeriod[1] + "\t" + timePeriod[2] + "\t" + timePeriod[3] + "\t" + timePeriod[4] + "\t" + timePeriod[5] + "\t" + timePeriod[6] + "\t" + timePeriod[7];
            context.write(new Text(calling_nbr), new Text(str));
            context.write(new Text(called_nbr), new Text(str));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double[] timePeriod = {0, 0, 0, 0, 0, 0, 0, 0};
            StringBuilder outputStr = new StringBuilder();
            for (Text value : values) {
                String line = value.toString();
                String[] srcData = line.split("\t");
                timePeriod[0] = Integer.parseInt(srcData[0]) + timePeriod[0];
                timePeriod[1] = Integer.parseInt(srcData[1]) + timePeriod[1];
                timePeriod[2] = Integer.parseInt(srcData[2]) + timePeriod[2];
                timePeriod[3] = Integer.parseInt(srcData[3]) + timePeriod[3];
                timePeriod[4] = Integer.parseInt(srcData[4]) + timePeriod[4];
                timePeriod[5] = Integer.parseInt(srcData[5]) + timePeriod[5];
                timePeriod[6] = Integer.parseInt(srcData[6]) + timePeriod[6];
                timePeriod[7] = Integer.parseInt(srcData[7]) + timePeriod[7];
            }


            double rate = 0;
            for (int i = 0; i < timePeriod.length; i++) {
                rate = timePeriod[i] / Arrays.stream(timePeriod).sum();
                outputStr.append(rate);
                if (i != timePeriod.length - 1) {
                    outputStr.append(" ");
                }
            }

            context.write(key, new Text(outputStr.toString()));
        }
    }

}
