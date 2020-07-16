package com.google.examples.dfdedup;

import com.google.examples.dfdedup.common.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.time.OffsetDateTime;
//import java.time.LocalDateTime;


public class HourlyAnalyze {
    @DefaultCoder(AvroCoder.class)
    public static class VendorInfo {
        String vendor_id;
        Integer passenger_count;
        Instant timestamp;

        public VendorInfo() {}

        public VendorInfo(String vendor_id, Integer passenger_count, Instant timestamp) {
            this.vendor_id = vendor_id;
            this.passenger_count = passenger_count;
            this.timestamp = timestamp;
        }

        public String getVendorId() { return vendor_id; }
        public Integer getPassengerCount() { return passenger_count; }
        public Instant getTimestamp() { return timestamp; }
    }

    public static class PrintAsStringFn extends SimpleFunction<String, String> {
        @Override
        public String apply(String input) {
            System.out.println(input);
            return input;
        }
    }

    static class ParsePostgresEventFn extends DoFn<KV<OffsetDateTime, String>, VendorInfo> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Instant timestamp = new Instant(c.element().getKey().toInstant().toEpochMilli());
            String[] components = c.element().getValue().split(",", -1);
            VendorInfo vInfo = new VendorInfo(components[0], Integer.parseInt(components[1]), timestamp);
            c.output(vInfo);
        }
    }

    static class ParsePubSubEventFn extends DoFn<Message, VendorInfo> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Instant timestamp = new Instant(c.element().getDropoffDatetimeAsInstant().toEpochMilli());
            String vendor_id = c.element().getVendorId();
            Integer passenger_count = c.element().getPassageCount();
            VendorInfo vInfo = new VendorInfo(vendor_id, passenger_count, timestamp);
            c.output(vInfo);
        }
    }

    public static class ExtractAndSumPassengerCount
        extends PTransform<PCollection<VendorInfo>, PCollection<KV<String, Integer>>> {

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<VendorInfo> vendorInfo) {
            return vendorInfo
                .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                    .via((VendorInfo vInfo) -> KV.of(vInfo.getVendorId(), vInfo.getPassengerCount())))
                .apply(Sum.integersPerKey());
        }
    }

    public static class FormatResult extends DoFn<KV<String, Integer>, String> {
        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            String result = window.start() + " , "+ c.element().getKey() + ": " + c.element().getValue();
            c.output(result);
        }
    }

    public static class FormatStreamResult extends DoFn<KV<String, Integer>, VendorInfo> {
        @ProcessElement
        public void processElement(ProcessContext c, IntervalWindow window) {
            VendorInfo result = new VendorInfo(c.element().getKey() ,c.element().getValue(), window.start());
            c.output(result);
        }
    }

//    protected static Map<String, WriteResultToPostgres.FieldInfo<KV<String, Integer>>> configureOutput() {
//        Map<String, WriteResultToPostgres.FieldInfo<KV<String, Integer>>> config = new HashMap<>();
//
//        config.put("window_start", new WriteResultToPostgres.FieldInfo<>((c, w) ->
//            { IntervalWindow window = (IntervalWindow) w;
//              return window.start();
//        }));
//        config.put("vendor_id", new WriteResultToPostgres.FieldInfo<>((c, w) -> c.element().getKey()));
//        config.put("passenger_count", new WriteResultToPostgres.FieldInfo<>((c, w) -> c.element().getValue()));
//        return config;
//    }

    static class StreamSumPassengerCount
        extends PTransform<PCollection<VendorInfo>, PCollection<KV<String, Integer>>> {

        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<VendorInfo> vInfo) {
            return vInfo
              .apply(Window.<VendorInfo>into(FixedWindows.of(Duration.standardMinutes(60)))
                  .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withEarlyFirings(
                               AfterProcessingTime.pastFirstElementInPane()
                                   .plusDelayOf(Duration.standardMinutes(1)))
                          .withLateFirings(
                               AfterProcessingTime.pastFirstElementInPane()
                                   .plusDelayOf(Duration.standardMinutes(10))))
                          .withAllowedLateness(Duration.standardMinutes(120))
                          .accumulatingFiredPanes())
              .apply(new ExtractAndSumPassengerCount());
        }
    }

    public static void main(String[] args) {

            DedupPipelineOptions dfOptions = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(DedupPipelineOptions.class);

            String projectId = dfOptions.getProject();

            Pipeline p = Pipeline.create(dfOptions);

//            PCollection<KV<OffsetDateTime, String>> psReadFromPostgres =
//              p.apply("ReadFromPostgres", new ReadFromPostgres());

            PCollection<Message> psReadForDedup =
                p.apply("DedupReadFromPubSub", new ReadFromPubSub(projectId,
                    dfOptions.getPubSubSubscriptionForDedup(), "logical_id", true));

//            psReadFromPostgres
//                .apply("ParseGameEvent", ParDo.of(new ParsePostgresEventFn()))
//                .apply("AddEventTimestamps", WithTimestamps.of((VendorInfo i) -> i.getTimestamp()))
//                .apply("FixedWindowsTeam", Window.into(FixedWindows.of(Duration.standardMinutes(30))))
//                .apply("ExtractAndSumPassengerCount", new ExtractAndSumPassengerCount())
//                .apply("FormatResult", ParDo.of(new FormatResult()))
//                .apply(MapElements.via(new PrintAsStringFn()));

            psReadForDedup
                .apply("ParseGameEvent", ParDo.of(new ParsePubSubEventFn()))
                .apply("AddEventTimestamps", WithTimestamps.of((VendorInfo i) -> i.getTimestamp()))
                .apply(new StreamSumPassengerCount())
                .apply("FormatResult", ParDo.of(new FormatStreamResult()))
                .apply(new WriteResultToPostgres());

            p.run().waitUntilFinish();

    }
}
