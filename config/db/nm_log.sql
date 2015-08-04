CREATE TABLE IF NOT EXISTS %snm_log (
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
