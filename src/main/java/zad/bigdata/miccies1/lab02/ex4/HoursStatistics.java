package zad.bigdata.miccies1.lab02.ex4;

import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class HoursStatistics extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HoursStatistics(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(this.getConf(), "Hours Statistics");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AppointmentInfo.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AppointmentInfo.class);

        job.setMapperClass(HoursStatisticsMap.class);
        // Setting job's combiner class - results coming to reducer are initially reduced
        job.setCombinerClass(HoursStatisticsReduce.class);
        job.setReducerClass(HoursStatisticsReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(zad.bigdata.miccies1.lab01.a.TitleCount.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Format needed to read dates from  file
    private static SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");

    /**
     * Gets time difference in unit specified in outputUnit argument.
     * @param end - end time
     * @param start - start time
     * @param outputUnit - Output unit
     * @return
     */
    private static long getDateDiff(Date end, Date start, TimeUnit outputUnit) {
        long diffInMillies = end.getTime() - start.getTime();
        return outputUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
    }

    /**
     * Parses given string as date.
     * @param dateString - string formatted accordingly to pattern set in frmtIn formatter.
     * @return Date object containing parsed date.
     * @throws ParseException
     */
    public static Date parseDate(String dateString) throws ParseException {
        return frmtIn.parse(dateString);
    }

    /**
     * Gets difference between end date and start date in minutes
     * @param end
     * @param start
     * @return
     */
    public static long getDateDiffMinutes(Date end, Date start) {
        return getDateDiff(end, start, TimeUnit.MINUTES);
    }

    /**
     * Gets difference between end date and start date in hours
     * @param end
     * @param start
     * @return
     */
    public static long getDateDiffHours(Date end, Date start) {
        return getDateDiff(end, start, TimeUnit.HOURS);
    }

    /**
     * Get specified date property
     * @param date - Date object of which the property is being got
     * @param calendarProperty - Integer, one of fields declared in Calendar class, for example Calendar.HOUR_OF_DAY
     * @return Value returned by calendar.get(calendarPropert) for given date.
     */
    public static int getDateProperty(Date date, int calendarProperty) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(calendarProperty);
    }

    public static class HoursStatisticsMap extends Mapper<Object, Text, IntWritable, AppointmentInfo> {

        /**
         * Mapper function. It checks whether the appointment time and arrival time properties are present,
         * validates if they differ by less than 24 hours. If the requirements are satisfied it writes the record
         * with hour as key and accordingly filled AppointmentInfo instance as value.
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                String[] data = line.split(",");
                if (!(data[11].equals("") || data[6].equals(""))) {
                    Date appointmentDate = parseDate(data[11]);
                    Date arrivalDate = parseDate(data[6]);
                    if (getDateDiffHours(arrivalDate, appointmentDate) < 24) {
                        IntWritable apointmentHour = new IntWritable(getDateProperty(appointmentDate, Calendar.HOUR_OF_DAY));
                        long latency = getDateDiffMinutes(arrivalDate, appointmentDate);
                        if (latency > 0) {
                            context.write(apointmentHour, AppointmentInfo.getInstanceLate(latency));
                        } else if (latency < 0) {
                            context.write(apointmentHour, AppointmentInfo.getInstancePrecipitate(-latency));
                        } else {
                            context.write(apointmentHour, AppointmentInfo.getInstanceOnTime());
                        }

                    }
                }
            } catch (Exception e) {
                Log LOG = LogFactory.getLog(HoursStatistics.class);
                LOG.warn("Could not parse given string as a date: " + e.getMessage());
            }
        }
    }

    public static class HoursStatisticsReduce extends Reducer<IntWritable, AppointmentInfo, IntWritable, AppointmentInfo> {

        /**
         * Calculates new AppointmentInfo, based on previously calculated informations
         * @param key Hour of day
         * @param values Iterable of AppointmentInfo objects created in mapper or combiner
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(IntWritable key, Iterable<AppointmentInfo> values, Context context) throws IOException, InterruptedException {
            int appointmentsCount = 0;
            int lateArrivalsCount = 0;
            int precipitationsCount = 0;

            // Sums need to be calculated, so the mean values do not depend on slice size
            double latenciesSum = 0d;
            double precipitationsSum = 0d;

            for(AppointmentInfo appointmentInfo : values) {
                appointmentsCount += appointmentInfo.getAppointmentsCount();
                lateArrivalsCount += appointmentInfo.getLateArrivalsCount();
                precipitationsCount += appointmentInfo.getPrecipitationsCount();

                latenciesSum += appointmentInfo.getMeanArrivalLatency() * appointmentInfo.getLateArrivalsCount();
                precipitationsSum += appointmentInfo.getMeanPrecipitation() * appointmentInfo.getPrecipitationsCount();
            }

            // Mean values are calculated when writing objects into context.
            context.write(key, new AppointmentInfo(
                    appointmentsCount,
                    appointmentsCount == 0 ? 0 : (latenciesSum + precipitationsSum) / appointmentsCount,
                    lateArrivalsCount,
                    lateArrivalsCount == 0 ? 0 : latenciesSum / lateArrivalsCount,
                    precipitationsCount,
                    precipitationsCount == 0 ? 0 : precipitationsSum / precipitationsCount
                    )
            );
        }
    }

    @Getter
    public static class AppointmentInfo implements Writable {

        public AppointmentInfo() { }


        public AppointmentInfo(int appointmentsCount, double meanTimesDifference, int lateArrivalsCount, double meanArrivalLatency, int precipitationsCount, double meanPrecipitation) {
            this.appointmentsCount = appointmentsCount;
            this.meanTimesDifference = meanTimesDifference;
            this.lateArrivalsCount = lateArrivalsCount;
            this.meanArrivalLatency = meanArrivalLatency;
            this.precipitationsCount = precipitationsCount;
            this.meanPrecipitation = meanPrecipitation;
        }

        /**
         * Get instance containing info about appointment which took place on time
         * @return
         */
        public static AppointmentInfo getInstanceOnTime() {
            return new AppointmentInfo(1, 0d, 0, 0d, 0, 0d);
        }

        /**
         * Get instance containing info about appointment which took place late
         * @param minutes
         * @return
         */
        public static AppointmentInfo getInstanceLate(long minutes) {
            return new AppointmentInfo(1, (double)minutes, 1, (double)minutes, 0, 0d);
        }

        /**
         * Get instance containing info about appointment which took place early
         * @param minutes
         * @return
         */
        public static AppointmentInfo getInstancePrecipitate(long minutes) {
            return new AppointmentInfo(1, (double)minutes, 0, 0d, 1, (double)minutes);
        }

        /**
         * Writable interface method implementation
         * @param dataOutput
         * @throws IOException
         */
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(appointmentsCount);
            dataOutput.writeDouble(meanTimesDifference);
            dataOutput.writeInt(lateArrivalsCount);
            dataOutput.writeDouble(meanArrivalLatency);
            dataOutput.writeInt(precipitationsCount);
            dataOutput.writeDouble(meanPrecipitation);
        }

        /**
         * Writable interface method implementation
         * @param dataInput
         * @throws IOException
         */
        public void readFields(DataInput dataInput) throws IOException {
            appointmentsCount = dataInput.readInt();
            meanTimesDifference = dataInput.readDouble();
            lateArrivalsCount = dataInput.readInt();
            meanArrivalLatency = dataInput.readDouble();
            precipitationsCount = dataInput.readInt();
            meanPrecipitation = dataInput.readDouble();
        }

        private int appointmentsCount;
        private double meanTimesDifference;
        private int lateArrivalsCount;
        private double meanArrivalLatency;
        private int precipitationsCount;
        private double meanPrecipitation;

        @Override
        public String toString() {
            return  "appointmentsCount=" + appointmentsCount +
                    ", meanTimesDifference=" + meanTimesDifference +
                    ", lateArrivalsCount=" + lateArrivalsCount +
                    ", meanArrivalLatency=" + meanArrivalLatency +
                    ", precipitationsCount=" + precipitationsCount +
                    ", meanPrecipitation=" + meanPrecipitation;
        }
    }
}