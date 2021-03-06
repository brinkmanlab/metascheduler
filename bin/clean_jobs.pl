#!/usr/bin/env perl

# Clean up old metascheduler jobs

$|++;

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Log::Log4perl;
use File::Path qw(remove_tree);

# Set connection information here
my $host = 'controlbk';
my $port = 7709;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use MetaScheduler::Config;
use MetaScheduler::DBISingleton;

my $scheduler; my $cfg;

MAIN: {
    my $cfname; my $days; my $logger;
    my $res = GetOptions("config=s" => \$cfname,
                         "days=s" => \$days,
    );

    # Find the config file and make sure we can open it
    $cfname |= '../etc/metascheduler.conf';
    die "Error, no configuration file found" 
        unless(-f $cfname && -r $cfname);

    # Initialize the scheduler
    MetaScheduler::Config->initialize({cfg_file => $cfname });
#    $scheduler = MetaScheduler->new({cfg_file => $cfname});

    # Get a local copy of the config so we can make a pid file
    $cfg = MetaScheduler::Config->config;

    MetaScheduler::DBISingleton->initialize({dsn => $cfg->{'dsn'}, 
					     user => $cfg->{'dbuser'}, 
					     pass => $cfg->{'dbpass'} });

    # Initialize the logging
    my $log_cfg = $cfg->{'logger_conf'};
    die "Error, can't access logger_conf $log_cfg"
    unless(-f $log_cfg && -r $log_cfg);

    Log::Log4perl::init($log_cfg);
    $logger = Log::Log4perl->get_logger;

    my $expiredays = 7;
    $expiredays = $days if($days);
    $logger->info("Expiring from days: $expiredays\n");

    my $dbh = MetaScheduler::DBISingleton->dbh;

my $find_records = $dbh->prepare("SELECT task_id from task WHERE submitted_date <= DATE_SUB(now(), INTERVAL ? DAY)");

    $find_records->execute($expiredays);

    while(my @rows = $find_records->fetchrow_array) {
	my $task_id = $rows[0];
	$logger->info("Deleting task_id $task_id");
	
        my $jobdir = $cfg->{jobs_dir} . '/' . $task_id;
        if(-d $jobdir) {
            $logger->info("Purging job dir $jobdir");
            remove_tree($jobdir);
        }

	$dbh->do("DELETE FROM mail WHERE task_id = ?", undef, $task_id);
	$dbh->do("DELETE FROM component WHERE task_id = ?", undef, $task_id);
	$dbh->do("DELETE FROM task WHERE task_id = ?", undef, $task_id);
    }

    $logger->info("Finished deleting");

}
