#!/usr/bin/env perl

$|++;

use strict;
use Cwd qw(abs_path getcwd);
use Getopt::Long;

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use MetaScheduler::ProcessReq;
use MetaScheduler;
use Data::Dumper;

#MetaScheduler::ProcessReq->initialize;

#MetaScheduler::ProcessReq->dispatch("submit", "blah");

my $cfname;
my $res = GetOptions("config=s" => \$cfname
    );

my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

open (JSON, "<../docs/sample.json") || die "Error, can not open ../docs/sample.json";

my $json = do {local $/;<JSON>};
close JSON;

MetaScheduler::ProcessReq->process_request($metascheduler, $json);


