CREATE TABLE IF NOT EXISTS %snm_plog (
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
