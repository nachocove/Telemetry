CREATE TABLE IF NOT EXISTS %snm_protocol (
 "id" varchar(64) not null unique primary key,
 "event_type" varchar(64) not null,
 "timestamped" timestamp,
 "user_id" varchar(64),
 "device_id" varchar(64),
 "payload" varchar(65535)
);
