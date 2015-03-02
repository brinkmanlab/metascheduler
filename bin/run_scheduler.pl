#!/usr/bin/env perl

$|++;

# Catch sigint (ctrl-c) and handle properly
$SIG{'INT'} = 'INT_handler';

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;
use Log::Log4perl;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use MetaScheduler;
use MetaScheduler::Server;

my $scheduler; my $fastload;

MAIN: {
    my $cfname;
    my $res = GetOptions("config=s" => \$cfname,
			 "fastload" => \$fastload,
    );

    # Find the config file and make sure we can open it
    $cfname |= '../etc/metascheduler.conf';
    die "Error, no configuration file found" 
        unless(-f $cfname && -r $cfname);

    # Initialize the scheduler
    $scheduler = MetaScheduler->new({cfg_file => $cfname,
				     fastload => $fastload});

    # Get a local copy of the config so we can make a pid file
    my $cfg = $scheduler->getCfg;

    # Make the PID file, probably not needed, but let's make one
    writePID($cfg->{pid_file});

    eval {
	$scheduler->runScheduler;
    };
    if($@) {
	print "Error!  This should never happen! $@\n";
    }

    # Clean up after ourselves
    unlink $cfg->{pid_file};

};

sub writePID {
    my $pid_file = shift;

    open PID, ">$pid_file"
	or die "Error, can't open pid file $pid_file: $@";

    print PID "$$\n";
    close PID;
}

sub INT_handler {
    print STDERR "sigint received, shutting down.\n";

    if($scheduler) {
	$SIG{'INT'} = 'INT_handler';
	$scheduler->finish;
	return;
    } else {
	print "scheduler doesn't seem to be started yet, goodbye\n";
	exit;
    }
}
