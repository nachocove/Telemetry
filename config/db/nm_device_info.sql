CREATE TABLE IF NOT EXISTS %snm_device_info (
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
