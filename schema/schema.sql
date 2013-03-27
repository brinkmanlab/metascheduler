CREATE DATABASE metascheduler;

CREATE TABLE IF NOT EXIST task (
  task_id int unsigned NOT NULL auto_increment,
  run_status enum('PENDING','COMPLETE','HOLD','ERROR','RUNNING'),
  job_id int unsigned not null,
  job_type VARCHAR(15) not null,
  extra_parameters VARCHAR(30),
  priority int unsigned default 2,
  submitted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  start_date TIMESTAMP NOT NULL,
  complete_date TIMESTAMP NOT NULL,
  PRIMARY KEY (task_id)
);

CREATE TABLE IF NOT EXIST component (
  component_id int unsigned NOT NULL auto_increment,
  task_id int unsigned NOT NULL,
  run_status enum('PENDING','COMPLETE','HOLD','ERROR','RUNNING'),
  extra_parameters VARCHAR(30),
  qsub_file VARCHAR(100),
  qsub_id int unsigned,
  start_date TIMESTAMP NOT NULL,
  complete_date TIMESTAMP NOT NULL,
  PRIMARY KEY (component_id)
);

CREATE TABLE IF NOT EXIST mail (
  mail_id int unsigned NOT NULL auto_increment,
  task_id int unsigned NOT NULL,
  email varchar(40) NOT NULL,
  sent bool DEFAULT 0,
  added_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_date TIMESTAMP NOT NULL,
  PRIMARY KEY (mail_id)
);

