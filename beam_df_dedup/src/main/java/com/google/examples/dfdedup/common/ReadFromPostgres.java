package com.google.examples.dfdedup.common;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;
import java.time.OffsetDateTime;

public class ReadFromPostgres extends PTransform<PBegin, PCollection<KV<OffsetDateTime, String>>> {

    public ReadFromPostgres() {}

    @Override
    public PCollection<KV<OffsetDateTime, String>> expand(PBegin begin) {
         return begin.apply(JdbcIO.<KV<OffsetDateTime, String>>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                "org.postgresql.Driver", "jdbc:postgresql://XXX/prac")
                .withUsername("XXX")
                .withPassword("YYY"))
            .withQuery("select dropoff_datetime, vendor_id, passenger_count from test20151019")
            .withRowMapper(new JdbcIO.RowMapper<KV<OffsetDateTime, String>>() {
                public KV<OffsetDateTime, String> mapRow(ResultSet resultSet) throws Exception {
                  return KV.of(
//                    resultSet.getTimestamp("dropoff_datetime"),
                    resultSet.getObject("dropoff_datetime", OffsetDateTime.class),
                    resultSet.getString("vendor_id") + "," +
                    resultSet.getString("passenger_count"));
                }
            })
            .withCoder(KvCoder.of(SerializableCoder.of(OffsetDateTime.class), StringUtf8Coder.of()))
        );
    }
}
