CREATE TABLE IF NOT EXISTS %snm_ui (
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
