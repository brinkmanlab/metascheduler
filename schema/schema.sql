CREATE DATABASE metascheduler;

CREATE TABLE IF NOT EXIST task (
  task_id int unsigned NOT NULL auto_increment,
  run_status enum('PENDING','COMPLETE','HOLD','ERROR','RUNNING'),
  job_id int unsigned not null,
  config_type VARCHAR(15) not null,
  extra_parameters VARCHAR(30),
  priority int unsigned default 2,
  PRIMARY KEY (task_id)
);

CREATE TABLE IF NOT EXIST component (
  component_id int unsigned NOT NULL auto_increment,
  task_id int unsigned NOT NULL,
  run_status enum('PENDING','COMPLETE','HOLD','ERROR','RUNNING'),
  extra_parameters VARCHAR(30),
  qsub_file VARCHAR(100),
  qsub_id int unsigned

);

CREATE TABLE IF NOT EXIST mail (
  mail_id int unsigned NOT NULL auto_increment,
  task_id int unsigned NOT NULL,
  email varchar(40) NOT NULL,
  sent tinyint(1) DEFAULT 0,
  
);

