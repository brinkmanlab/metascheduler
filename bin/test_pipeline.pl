#!/usr/bin/perl

use warnings;
use strict;
use Cwd qw(abs_path getcwd);

BEGIN{
# Find absolute path of script
my ($path) = abs_path($0) =~ /^(.+)\//;
chdir($path);
sub mypath { return $path; }
};

use lib "../lib";
use Pipeline;
use Job;
use DBISingleton;

my $dbhs = DBISingleton->initialize({dsn => "DBI:mysql:database=metascheduler;host=localhost", user => "scheduler", pass => "sched44%"});

my $dbh = $dbhs->dbh;
#my $dbh = DBISingleton->dbh;

#$dbh->do("SELECT SLEEP(10)");
#sleep 20;

my $pipeline = Pipeline->new;

my $res = $pipeline->read_pipeline("/home/lairdm/metascheduler/docs/sample.config");
print "$res\n";
$pipeline->dump();

my $json;
{
    local $/; #enable slurp
    open my $fh, "<", "/home/lairdm/metascheduler/docs/sample.job";
    $json = <$fh>;
} 

my $job = Job->new(job => $json);
$job->dump();
