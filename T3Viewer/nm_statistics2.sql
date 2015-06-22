CREATE TABLE %s_nm_statistics2 (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "stat2_name" varchar(64),
 "max" int,
 "min" int,
 "sum" int,
 "sum2" int,
 "count" int
);
