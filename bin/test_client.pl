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
use MetaScheduler::Client;

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

    my $client =  MetaScheduler::Client->new();

    $client->connect('localhost', 7709);

    my $results = $client->send_req("Test sending\n");

    print "We received back:\n$results";
}
