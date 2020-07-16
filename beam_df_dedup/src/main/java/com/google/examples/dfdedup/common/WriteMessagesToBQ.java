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
// limitations under the License.getTimestampAsInstant

package com.google.examples.dfdedup.common;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class WriteMessagesToBQ extends PTransform<PCollection<Message>, WriteResult> {

    private String bqTableSpec;
    private String dedupType;

    public WriteMessagesToBQ(String projectId,
                             String bqDataset,
                             String destTable,
                             String dedupType) {
        StringBuilder sbTableSpec = new StringBuilder();

        sbTableSpec
                .append(projectId).append(":")
                .append(bqDataset).append(".")
                .append(destTable);

        this.bqTableSpec = sbTableSpec.toString();
        this.dedupType = dedupType;
    }

    @Override
    public WriteResult expand(PCollection<Message> input) {

        BigQueryIO.Write.Method writeMethod = BigQueryIO.Write.Method.STREAMING_INSERTS;

        return input.apply(BigQueryIO.<Message>write()
                .to(this.bqTableSpec)
                .withSchema(new TableSchema().setFields(
                        ImmutableList.of(
                                new TableFieldSchema().setName("vendor_id").setType("STRING"),
                                new TableFieldSchema().setName("pickup_datetime").setType("TIMESTAMP"),
                                new TableFieldSchema().setName("dropoff_datetime").setType("TIMESTAMP"),
                                new TableFieldSchema().setName("passenger_count").setType("Integer"),
                                new TableFieldSchema().setName("trip_distance").setType("Float"),
                                new TableFieldSchema().setName("pickup_longitude").setType("Float"),
                                new TableFieldSchema().setName("pickup_latitude").setType("Float"),
                                new TableFieldSchema().setName("rate_code").setType("Integer"),
                                new TableFieldSchema().setName("store_and_fwd_flag").setType("String"),
                                new TableFieldSchema().setName("dropoff_longitude").setType("Float"),
                                new TableFieldSchema().setName("dropoff_latitude").setType("Float"),
                                new TableFieldSchema().setName("payment_type").setType("String"),
                                new TableFieldSchema().setName("fare_amount").setType("Float"),
                                new TableFieldSchema().setName("extra").setType("Float"),
                                new TableFieldSchema().setName("mta_tax").setType("Float"),
                                new TableFieldSchema().setName("tip_amount").setType("Float"),
                                new TableFieldSchema().setName("tolls_amount").setType("Float"),
                                new TableFieldSchema().setName("imp_surcharge").setType("Float"),
                                new TableFieldSchema().setName("total_amount").setType("Float"))))
                .withFormatFunction(msg -> new TableRow()
                        .set("vendor_id", msg.getVendorId())
                        .set("pickup_datetime", msg.getPickupDatetimeAsInstant().toEpochMilli() / 1000)
                        .set("dropoff_datetime", msg.getDropoffDatetimeAsInstant().toEpochMilli() / 1000)
                        .set("passenger_count", msg.getPassageCount())
                        .set("trip_distance", msg.getTripDistance())
                        .set("pickup_longitude", msg.getPickupLongitude())
                        .set("pickup_latitude", msg.getPickupLatitude())
                        .set("rate_code", msg.getRateCode())
                        .set("store_and_fwd_flag", msg.getStoreAndFwdFlag())
                        .set("dropoff_longitude", msg.getDropoffLatitude())
                        .set("dropoff_latitude", msg.getDropoffLatitude())
                        .set("payment_type", msg.getPaymentType())
                        .set("fare_amount", msg.getFareAmount())
                        .set("extra", msg.getExtra())
                        .set("mta_tax", msg.getMtaTax())
                        .set("tip_amount", msg.getTipAmount())
                        .set("tolls_amount", msg.getTollsAmount())
                        .set("imp_surcharge", msg.getImpSurcharge())
                        .set("total_amount", msg.getTotalAmount()))
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//                .withTimePartitioning(new TimePartitioning().setField("RunTimestamp").setType("TIMESTAMP"))
                .withMethod(writeMethod)
        );
    }
}
