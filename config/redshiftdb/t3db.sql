CREATE TABLE client_log (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "uploaded_at" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "thread_id" int,
 "module" varchar(64),
 "message" varchar(65535)
);
CREATE TABLE pinger_log (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "uploaded_at" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "session" varchar(64),
 "context" varchar(64),
 "module" varchar(64),
 "pinger" varchar(64),
 "message" varchar(256)
);
CREATE TABLE client_protocol (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "payload" varchar(65535)
);
CREATE TABLE client_ui (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "ui_type" varchar(64),
 "ui_object" varchar(64),
 "ui_string" varchar(128),
 "ui_long" int
);
CREATE TABLE client_device_info (
 "id" varchar(64) not null unique primary key,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "os_type" varchar(64),
 "os_version" varchar(64),
 "device_model" varchar(64),
 "build_version" varchar(64),
 "build_number" varchar(64),
 "fresh_install" boolean
);
CREATE TABLE client_counter (
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
CREATE TABLE client_statistics2 (
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

CREATE TABLE client_samples (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "samples_name" varchar(64),
 "sample_value" int
);

COMMIT;
