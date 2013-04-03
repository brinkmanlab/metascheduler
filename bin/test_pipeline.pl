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
use MetaScheduler::Pipeline;
#use MetaScheduler::Job;
use MetaScheduler::DBISingleton;

MAIN: {

    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

    my $pipeline = MetaScheduler::Pipeline->new({pipeline => "/home/lairdm/metascheduler/docs/sample.config"});

#    $pipeline->dump();

    print Dumper $pipeline->fetch_component('islandpick');

#my $json;
#{
#    local $/; #enable slurp
#    open my $fh, "<", "/home/lairdm/metascheduler/docs/sample.job";
#    $json = <$fh>;
#} 

};

#my $job = MetaScheduler::Job->new(job => $json);
#$job->dump();
