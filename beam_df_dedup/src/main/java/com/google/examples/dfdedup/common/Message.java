// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.examples.dfdedup.common;

import com.google.gson.Gson;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import java.math.BigDecimal;


@DefaultCoder(AvroCoder.class)
public class Message {
//    protected String logical_id;
//    public String getLogicalId() {
//        return logical_id;
//    }
//    public void setLogicalId(String logical_id) {
//        this.logical_id = logical_id;
//    }

    protected String vendor_id;
    public String getVendorId() {
        return vendor_id;
    }
    public void setVendorId(String vendor_id) {
        this.vendor_id = vendor_id;
    }

    protected String pickup_datetime;
    public String getPickupDatetime() {
        return this.pickup_datetime;
    }
    public void setPickupDatetime(String pickup_datetime) {
        this.pickup_datetime = pickup_datetime;
    }
    public Instant getPickupDatetimeAsInstant() {
        return getTimestampAsInstant(this.pickup_datetime);
    }

    protected String dropoff_datetime;
    public String getDropoffDatetime() {
        return this.dropoff_datetime;
    }
    public void setDropoffDatetime(String dropoff_datetime) {
        this.dropoff_datetime = dropoff_datetime;
    }
    public Instant getDropoffDatetimeAsInstant() {
        return getTimestampAsInstant(this.dropoff_datetime);
    }

    protected Integer passenger_count;
    public Integer getPassageCount() { return this.passenger_count; }
    public void setPassengerCount(Integer passenger_count) { this.passenger_count = passenger_count; }

    protected Float trip_distance;
    public Float getTripDistance() { return this.trip_distance; }
    public void setTripDistance(Float trip_distance) { this.trip_distance = trip_distance; }

    protected BigDecimal pickup_longitude;
    public BigDecimal getPickupLongitude() { return this.pickup_longitude; }
    public void setPickupLongitude(BigDecimal pickup_longitude) { this.pickup_longitude = pickup_longitude; }

    protected BigDecimal pickup_latitude;
    public BigDecimal getPickupLatitude() { return this.pickup_longitude; }
    public void setPickupLatitude(BigDecimal pickup_latitude) { this.pickup_longitude = pickup_latitude; }

    protected Integer rate_code;
    public Integer getRateCode() { return this.rate_code; }
    public void setRateCode(Integer rate_code) { this.rate_code = rate_code; }

    protected String store_and_fwd_flag;
    public String getStoreAndFwdFlag() { return this.store_and_fwd_flag; }
    public void setStoreAndFwdFlag(String store_and_fwd_flag) { this.store_and_fwd_flag = store_and_fwd_flag; }

    protected BigDecimal dropoff_longitude;
    public BigDecimal getDropoffLongitude() { return this.dropoff_longitude; }
    public void setDropoffLongitude(BigDecimal dropoff_longitude) { this.dropoff_longitude = dropoff_longitude; }

    protected BigDecimal dropoff_latitude;
    public BigDecimal getDropoffLatitude() { return this.dropoff_longitude; }
    public void setDropoffLatitude(BigDecimal pickup_latitude) { this.dropoff_longitude = dropoff_latitude; }

    protected String payment_type;
    public String getPaymentType() { return this.payment_type; }
    public void setPaymentType(String payment_type) { this.payment_type = payment_type; }

    protected Float fare_amount;
    public Float getFareAmount() { return this.fare_amount; }
    public void setFareAmount(Float fare_amount) { this.fare_amount = fare_amount; }

    protected Float extra;
    public Float getExtra() { return this.extra; }
    public void setExtra(Float extra) { this.extra = extra; }

    protected Float mta_tax;
    public Float getMtaTax() { return this.mta_tax; }
    public void setMtaTax(Float mta_tax) { this.mta_tax = mta_tax; }

    protected Float tip_amount;
    public Float getTipAmount() { return this.tip_amount; }
    public void setTipAmount(Float tip_amount) { this.tip_amount = tip_amount; }

    protected Float tolls_amount;
    public Float getTollsAmount() { return this.tolls_amount; }
    public void setTollsAmount(Float tolls_amount) { this.tolls_amount = tolls_amount; }

    protected Float imp_surcharge;
    public Float getImpSurcharge() { return this.imp_surcharge; }
    public void setImpSurcharge(Float imp_surcharge) { this.imp_surcharge = imp_surcharge; }

    protected Float total_amount;
    public Float getTotalAmount() { return this.total_amount; }
    public void setTotalAmount(Float total_amount) { this.total_amount = total_amount; }

//    public org.joda.time.Instant getEventTimeAsJodaInstant() {
//        return getTimestampAsJodaTimeInstant(this.EventTime);
//    }

    private Instant getTimestampAsInstant(String timestampStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        TemporalAccessor temporalAccessor = formatter.parse(timestampStr);
        LocalDateTime localDateTime = LocalDateTime.from(temporalAccessor);
//        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.of("UTC+0"));
        return Instant.from(zonedDateTime);
    }
//
//    private org.joda.time.Instant getTimestampAsJodaTimeInstant(String timestampStr) {
//        Instant i = getTimestampAsInstant(timestampStr);
//        return new org.joda.time.Instant(i.toEpochMilli());
//    }

    public String toJson(Gson gson) {
        return gson.toJson(this);
    }

    public static Message fromJson(Gson gson, String rawJson) {
        return gson.fromJson(rawJson, Message.class);
    }
}
