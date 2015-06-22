CREATE TABLE IF NOT EXISTS %snm_counter (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "counter_name" varchar(64),
 "counter_start" timestamp,
 "counter_end" timestamp,
 "count" int
);
