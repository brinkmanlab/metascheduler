#!/usr/bin/env perl

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
use MetaScheduler::Server;

my $logger;
my $cfg;

MAIN: {
    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    # Initialize the configuration file
    MetaScheduler::Config->initialize({cfg_file => $cfname});
    $cfg =  MetaScheduler::Config->config;

    my $log_cfg = $cfg->{'logger_conf'};
    Log::Log4perl::init($log_cfg);
    $logger = Log::Log4perl->get_logger;

    MetaScheduler::Server->initialize();

    my $server =  MetaScheduler::Server->instance;

    my $metascheduler = MetaScheduler->new;

    $metascheduler->process_request("Test a request\n");

    while(1) {
	if($server->reqs_waiting) {
	    $server->process_requests($metascheduler);
	}
	sleep 1;
    }
}

package MetaScheduler;

sub new {
    my $class= shift;
    
    my $self = {};
    bless $self, $class;
    return $self;
}

sub process_request {
    my $self = shift;
    my $req = shift;

    print "Processing request from server:\n$req";

    return "Thank you, we've processed your request\n";
}
