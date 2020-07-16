package com.google.examples.dfdedup.common;

import com.google.examples.dfdedup.HourlyAnalyze.VendorInfo;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;


public class WriteResultToPostgres extends PTransform<PCollection<VendorInfo>, PDone> {
    @Override
    public PDone expand(PCollection<VendorInfo> input) {
      return input

        .apply(JdbcIO.<VendorInfo>write()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
          "org.postgresql.Driver", "jdbc:postgresql://XXX:5432/prac")
          .withUsername("XXX")
          .withPassword("YYY"))
        .withStatement("insert into new_york_yellow_taxi_aggregate" +
          "(window_start, vendor_id, passenger_count)" +
          "values (?, ?, ?)")
        .withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<VendorInfo>() {
          public void setParameters(VendorInfo r, PreparedStatement query)
            throws SQLException {
            Instant instant = Instant.ofEpochMilli(r.getTimestamp().getMillis());
            OffsetDateTime dateTime = OffsetDateTime.ofInstant(instant, ZoneId.of("UTC-8"));
//            Timestamp timeStamp = new Timestamp(dateTime.getMillis());
            query.setObject(1, dateTime);
            query.setString(2, (String) r.getVendorId());
            query.setInt(3, (Integer) r.getPassengerCount());
          }
        })
      );
    }
}
