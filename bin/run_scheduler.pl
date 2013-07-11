#!/usr/bin/env perl

# Catch sigint (ctrl-c) and handle properly
$SIG{'INT'} = 'INT_handler';

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

my $scheduler;

MAIN: {
    
}

sub INT_handler {
    print "sigint received, shutting down.\n";

    if($scheduler) {
	$SIG{'INT'} = 'INT_handler';
	$scheduler->finish;
	return;
    } else {
	print "scheduler doesn't seem to be started yet, goodbye\n";
	exit;
    }
}
