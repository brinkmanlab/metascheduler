CREATE DATABASE metascheduler;

GRANT USAGE ON *.* TO 'scheduler'@'%' IDENTIFIED BY PASSWORD '*21AF0E289C82A0CBC5B34B8DF3B14594C09F5C16'
GRANT SELECT, INSERT, UPDATE, DELETE, LOCK TABLES ON `metascheduler`.* TO 'scheduler'@'%'

CREATE TABLE IF NOT EXISTS task (
  task_id int unsigned NOT NULL auto_increment,
  run_status enum('PENDING','COMPLETE','HOLD','ERROR','RUNNING','DELETED') DEFAULT 'PENDING',
  job_id int unsigned not null,
  job_type VARCHAR(15) not null,
  job_name VARCHAR(25),
  extra_parameters VARCHAR(30)  NOT NULL DEFAULT '',
  priority int unsigned default 2,
  job_scheduler VARCHAR(15) NOT NULL,
  submitted_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  start_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  complete_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (task_id),
  UNIQUE idx_ext_refs (job_type, job_id)
);

CREATE TABLE IF NOT EXISTS component (
  component_id int unsigned NOT NULL auto_increment,
  task_id int unsigned NOT NULL,
  component_type varchar(30) NOT NULL,
  run_status enum('PENDING','COMPLETE','HOLD','ERROR','RUNNING') DEFAULT 'PENDING',
  extra_parameters VARCHAR(30) NOT NULL DEFAULT '',
  qsub_file VARCHAR(100) DEFAULT '',
  qsub_id int unsigned default 0,
  start_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  complete_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  PRIMARY KEY (component_id),
  UNIQUE idx_ext_task (task_id, component_type)
);

CREATE TABLE IF NOT EXISTS mail (
  mail_id int unsigned NOT NULL auto_increment,
  task_id int unsigned NOT NULL,
  email varchar(40) NOT NULL,
  sent bool DEFAULT 0,
  added_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sent_date TIMESTAMP NOT NULL,
  PRIMARY KEY (mail_id)
);

