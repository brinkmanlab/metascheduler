#!/usr/bin/env perl

# The client for accessing MetaScheduler
#
# It uses the client access library and calls
# specific task modules depending what command
# is issued.

$|++;

my $actions = {
    submit => 'submit',
    show   => 'show',
    alter  => 'alter',
    delete => 'delete',
    status => 'status',
    graph  => 'graph',
    admin  => 'admin',
    logging => 'logging',
    refresh => 'refresh',
    timeout => 'timeout',
    remove => 'remove',
};

use warnings;
use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;

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
use MetaScheduler::Client;

# Start the script

# First find what command we're issuing
my $command = lc shift @ARGV;

# Because authentication isn't setup yet we're
# just going to push those parameters on so users
# don't have to
push @ARGV, '-u', 'user', '-p', 'pass';

unless($command && $actions->{$command}) {
    print "Error, unknown command\n";
    usage();
    exit 1;
}

# Try and load the module
eval {
    no strict 'refs';
    require "MetaScheduler/Client/$command.pm";
    "MetaScheduler::Client::$command"->initialize($host, $port, @ARGV);
};

if($@) {
    die "Error, unknown command $command: $@";
}

exit;

sub usage {
    print "Usage: $0 [command] [args]\n";
    print "   Use $0 [command] -h for arguments to each command\n\n";
    print "  Allowed commands:\n";
    foreach my $cmd (keys %{$actions}) {
	print "    $cmd\n";
    }
}
