#!/usr/bin/perl

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
use MetaScheduler::Job;
use MetaScheduler::Component;
use MetaScheduler::DBISingleton;

MAIN: {

    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

    my $json;
    {
	local $/;; #enable slurp
	open my $fh, "<", "/home/lairdm/metascheduler/docs/sample.job";
	$json = <$fh>;
    }

    my $job = MetaScheduler::Job->new({job => $json
					    });

    $job->add_emails('lairdm@luther.ca', 'blah@there');

$job->change_state({state => "RUNNING",
		    component_type => 'cvtree',
		    qsub_id => 333
		   });
    $job->change_state({state => "RUNNING"});

    print $job->dump();

    my $task_id = $job->task_id;
    print "Task_id $task_id\nReloading\n";

    my $job2 = MetaScheduler::Job->new({task_id => $task_id});

    print $job2->dump();

    my $jobs = MetaScheduler::Job->find_jobs('IslandViewer');

    print Dumper $jobs;
};
