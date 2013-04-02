#!/usr/bin/perl

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

use lib "../lib";
use MetaScheduler;
use MetaScheduler::DBISingleton;

MAIN: {
    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

}
