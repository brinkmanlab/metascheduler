#!/usr/bin/env perl

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Data::Dumper;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use MetaScheduler;
use MetaScheduler::Torque;
use MetaScheduler::Torque::QStat;
use MetaScheduler::Config;

MAIN: {

    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

#    MetaScheduler::Torque->initialize();

    MetaScheduler::Torque::QStat->refresh({force => 1});

print "Dumping job:\n";
#print Dumper 
#my $job = MetaScheduler::Torque::QStat->get_job("11526266.b0");
#print Dumper $job;
#print Dumper 
my $job = MetaScheduler::Torque::QStat->fetch("11526266.b0");
print Dumper $job;

print "Job state: " .  MetaScheduler::Torque->fetch_job_state("11526266.b0") . "\n";

my $job_id = MetaScheduler::Torque->submit_job("test_job", "/my/qsub/file.qsub", "/jobdir");

print "We received job number $job_id\n";

print "Job state: " .  MetaScheduler::Torque->fetch_job_state("$job_id") . "\n";

}

