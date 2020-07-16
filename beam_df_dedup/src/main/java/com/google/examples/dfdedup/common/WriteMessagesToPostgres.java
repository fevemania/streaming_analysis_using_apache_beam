package com.google.examples.dfdedup.common;
//
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class WriteMessagesToPostgres extends PTransform<PCollection<Message>, PDone>  {

  public WriteMessagesToPostgres() {}

  @Override
  public PDone expand(PCollection<Message> input) {
      return input.apply(JdbcIO.<Message>write()
        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
          "org.postgresql.Driver", "jdbc:postgresql://XXX/prac")
          .withUsername("XXX")
          .withPassword("YYY"))
        .withStatement("insert into test201510191900 " +
          "(vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance," +
          " pickup_longitude, pickup_latitude, rate_code, store_and_fwd_flag, dropoff_longitude," +
          " dropoff_latitude, payment_type, fare_amount, extra, mta_tax," +
          " tip_amount, tolls_amount, imp_surcharge, total_amount)" +
          "values (?, to_timestamp(?), to_timestamp(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .withPreparedStatementSetter(new PreparedStatementSetter<Message>() {
          public void setParameters(Message msg, PreparedStatement query)
            throws SQLException {
            query.setString(1, msg.getVendorId());
            query.setObject(2, msg.getPickupDatetimeAsInstant().toEpochMilli()/1000);
            query.setObject(3, msg.getDropoffDatetimeAsInstant().toEpochMilli()/1000);
            query.setInt(4, msg.getPassageCount());
            query.setFloat(5, msg.getTripDistance());
            query.setBigDecimal(6, msg.getPickupLongitude());
            query.setBigDecimal(7, msg.getPickupLatitude());
            query.setInt(8, msg.getRateCode());
            query.setString(9, msg.getStoreAndFwdFlag());
            query.setBigDecimal(10, msg.getDropoffLongitude());
            query.setBigDecimal(11, msg.getDropoffLatitude());
            query.setString(12, msg.getPaymentType());
            query.setFloat(13, msg.getFareAmount());
            query.setFloat(14, msg.getExtra());
            query.setFloat(15, msg.getMtaTax());
            query.setFloat(16, msg.getTipAmount());
            query.setFloat(17, msg.getTollsAmount());
            query.setFloat(18, msg.getImpSurcharge());
            query.setFloat(19, msg.getTotalAmount());
          }
        })
      );
  }

}
