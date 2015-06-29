CREATE TABLE IF NOT EXISTS %snm_samples (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "samples_name" varchar(64),
 "sample_int" int,
 "sample_string" varchar(256),
 "sample_float" float
);
