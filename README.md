# streaming_analysis_using_apache_beam

### Deduplicate Data from Pub/Sub, and analyze the sum of passenger count for each taxi vendor in the real time. Trigger out and write the aggregation result to data sink per minute

### Dataset: bigquery-public-dataset/new_york/tlc_yellow_trips_2015

### For demo purpose: I only choose to analyze the 24 hr inside 2015-10-19. (according to the dropoff_datetime field in the dataset).

### Below is the sample result.

#### First Minute (Aggregate data from unbound data source (i.e. Pub/Sub)

| id |    window_start     | vendor_id | passenger_count |                     
|----|---------------------|-----------|-----------------|                     
|  6 | 2015-10-19 00:00:00 | 1         |            3721 |
| 18 | 2015-10-19 01:00:00 | 1         |            2046 |
|  2 | 2015-10-19 02:00:00 | 1         |            1324 |
| 20 | 2015-10-19 03:00:00 | 1         |             915 |
|  4 | 2015-10-19 04:00:00 | 1         |             877 |
| 24 | 2015-10-19 05:00:00 | 1         |            1390 |
| 12 | 2015-10-19 06:00:00 | 1         |            4052 |
| 11 | 2015-10-19 07:00:00 | 1         |            6996 |
| 25 | 2015-10-19 08:00:00 | 1         |            8459 |
|  3 | 2015-10-19 09:00:00 | 1         |            7632 |
| 19 | 2015-10-19 10:00:00 | 1         |            7300 |
|  1 | 2015-10-19 11:00:00 | 1         |            7213 |
| 21 | 2015-10-19 12:00:00 | 1         |            7610 |
|  8 | 2015-10-19 13:00:00 | 1         |            7466 |
|  7 | 2015-10-19 14:00:00 | 1         |            8079 |
| 16 | 2015-10-19 15:00:00 | 1         |            8638 |
| 17 | 2015-10-19 16:00:00 | 1         |            7829 |
| 10 | 2015-10-19 17:00:00 | 1         |            8382 |
| 14 | 2015-10-19 18:00:00 | 1         |           10258 |
|  9 | 2015-10-19 19:00:00 | 1         |           11172 |
| 15 | 2015-10-19 20:00:00 | 1         |           10103 |
| 13 | 2015-10-19 21:00:00 | 1         |            9892 |
| 22 | 2015-10-19 22:00:00 | 1         |            8604 |
| 23 | 2015-10-19 23:00:00 | 1         |            5969 |
| 48 | 2015-10-19 00:00:00 | 2         |            6474 |
| 40 | 2015-10-19 01:00:00 | 2         |            2987 |
|  5 | 2015-10-19 02:00:00 | 2         |            1977 |
| 41 | 2015-10-19 03:00:00 | 2         |            1325 |
| 28 | 2015-10-19 04:00:00 | 2         |            1217 |
| 47 | 2015-10-19 05:00:00 | 2         |            2535 |
| 33 | 2015-10-19 06:00:00 | 2         |            7065 |
| 34 | 2015-10-19 07:00:00 | 2         |           13675 |
| 46 | 2015-10-19 08:00:00 | 2         |           16267 |
| 27 | 2015-10-19 09:00:00 | 2         |           16115 |
| 42 | 2015-10-19 10:00:00 | 2         |           13804 |
| 26 | 2015-10-19 11:00:00 | 2         |           13303 |
| 43 | 2015-10-19 12:00:00 | 2         |           14079 |
| 30 | 2015-10-19 13:00:00 | 2         |           13446 |
| 29 | 2015-10-19 14:00:00 | 2         |           14233 |
| 38 | 2015-10-19 15:00:00 | 2         |           14649 |
| 39 | 2015-10-19 16:00:00 | 2         |           13623 |
| 32 | 2015-10-19 17:00:00 | 2         |           16027 |
| 36 | 2015-10-19 18:00:00 | 2         |           20332 |
| 31 | 2015-10-19 19:00:00 | 2         |           22384 |
| 37 | 2015-10-19 20:00:00 | 2         |           19971 |
| 35 | 2015-10-19 21:00:00 | 2         |           18331 |
| 44 | 2015-10-19 22:00:00 | 2         |           15857 |
| 45 | 2015-10-19 23:00:00 | 2         |           11052 |
 
 #### Second Minute (Aggregate data from unbound data source (i.e. Pub/Sub)
 
| id |    window_start     | vendor_id | passenger_count |
|----|---------------------|-----------|-----------------|
| 76 | 2015-10-19 00:00:00 | 1         |            5667 |
| 88 | 2015-10-19 01:00:00 | 1         |            3028 |
| 73 | 2015-10-19 02:00:00 | 1         |            2048 |
| 90 | 2015-10-19 03:00:00 | 1         |            1334 |
| 75 | 2015-10-19 04:00:00 | 1         |            1449 |
| 95 | 2015-10-19 05:00:00 | 1         |            2150 |
| 81 | 2015-10-19 06:00:00 | 1         |            5201 |
| 82 | 2015-10-19 07:00:00 | 1         |            8933 |
| 94 | 2015-10-19 08:00:00 | 1         |           11118 |
| 74 | 2015-10-19 09:00:00 | 1         |           10731 |
| 89 | 2015-10-19 10:00:00 | 1         |           10126 |
| 72 | 2015-10-19 11:00:00 | 1         |           10021 |
| 91 | 2015-10-19 12:00:00 | 1         |           10538 |
| 78 | 2015-10-19 13:00:00 | 1         |           10508 |
| 77 | 2015-10-19 14:00:00 | 1         |           11261 |
| 86 | 2015-10-19 15:00:00 | 1         |           12139 |
| 87 | 2015-10-19 16:00:00 | 1         |           10711 |
| 80 | 2015-10-19 17:00:00 | 1         |           11069 |
| 84 | 2015-10-19 18:00:00 | 1         |           13624 |
| 79 | 2015-10-19 19:00:00 | 1         |           14461 |
| 85 | 2015-10-19 20:00:00 | 1         |           13016 |
| 83 | 2015-10-19 21:00:00 | 1         |           12700 |
| 92 | 2015-10-19 22:00:00 | 1         |           11602 |
| 93 | 2015-10-19 23:00:00 | 1         |            8489 |
| 96 | 2015-10-19 00:00:00 | 2         |            9178 |
| 64 | 2015-10-19 01:00:00 | 2         |            4668 |
| 50 | 2015-10-19 02:00:00 | 2         |            2864 |
| 66 | 2015-10-19 03:00:00 | 2         |            1963 |
| 51 | 2015-10-19 04:00:00 | 2         |            2001 |
| 71 | 2015-10-19 05:00:00 | 2         |            3714 |
| 58 | 2015-10-19 06:00:00 | 2         |            9035 |
| 57 | 2015-10-19 07:00:00 | 2         |           17189 |
| 70 | 2015-10-19 08:00:00 | 2         |           21222 |
| 52 | 2015-10-19 09:00:00 | 2         |           22050 |
| 65 | 2015-10-19 10:00:00 | 2         |           19161 |
| 49 | 2015-10-19 11:00:00 | 2         |           18352 |
| 67 | 2015-10-19 12:00:00 | 2         |           19359 |
| 54 | 2015-10-19 13:00:00 | 2         |           18103 |
| 53 | 2015-10-19 14:00:00 | 2         |           19506 |
| 62 | 2015-10-19 15:00:00 | 2         |           20186 |
| 63 | 2015-10-19 16:00:00 | 2         |           18734 |
| 56 | 2015-10-19 17:00:00 | 2         |           21008 |
| 60 | 2015-10-19 18:00:00 | 2         |           26804 |
| 55 | 2015-10-19 19:00:00 | 2         |           28590 |
| 61 | 2015-10-19 20:00:00 | 2         |           25718 |
| 59 | 2015-10-19 21:00:00 | 2         |           24202 |
| 69 | 2015-10-19 22:00:00 | 2         |           21488 |
| 68 | 2015-10-19 23:00:00 | 2         |           15821 |
