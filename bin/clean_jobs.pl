#!/usr/bin/env perl

# Clean up old metascheduler jobs

$|++;

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Log::Log4perl;

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
use MetaScheduler;
use MetaScheduler::DBISingleton;

my $scheduler;

MAIN: {
    my $cfname;
    my $res = GetOptions("config=s" => \$cfname,
                         "days=s" => \$days,
    );

    # Find the config file and make sure we can open it
    $cfname |= '../etc/metascheduler.conf';
    die "Error, no configuration file found" 
        unless(-f $cfname && -r $cfname);

    # Initialize the scheduler
    $scheduler = MetaScheduler->new({cfg_file => $cfname});

    # Get a local copy of the config so we can make a pid file
    my $cfg = $scheduler->getCfg;

    $self->{logger} = Log::Log4perl->get_logger;

    my $expiredays = 7;
    $expire_days ||= $days;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my find_records = $dbi->prepare("SELECT task_id from task WHERE complete_date = DATE_SUB(now(), INTERVAL ? DAYS")

    $find_records->execute($expire_days);

    while(my @rows = $find_records->fetchrow_array) {
	my $task_id = @row[0];
	$logger->info("Deleting task_id $task_id");
	
	$dbh->do("DELETE FROM mail WHERE task_id = ?", undef, $task_id);
	$dbh->do("DELETE FROM component WHERE task_id = ?", undef, $task_id);
	$dbh->do("DELETE FROM task WHERE task_id = ?", undef, $task_id);
    }

    $logger->info("Finished deleting");

}
