CREATE TABLE IF NOT EXISTS %snm_time_series (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "time_series_timestamp" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "time_series_name" varchar(64),
 "time_series_int" int,
 "time_series_string" varchar(256),
 "time_series_float" float
);
