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
use MetaScheduler::Component;
use MetaScheduler::DBISingleton;

MAIN: {

    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

    my $component = MetaScheduler::Component->new({component_type => "cvtree",
						   qsub_file => "/home/shared/Modules/islandviewer/custom_jobs/56434/cvtree.qsub",
						   task_id => 1,
						  });

    my $component_pieces = { component_type => "islandpick",
			     qsub_file => "/home/shared/Modules/islandviewer/custom_jobs/56434/islandpick.qsub",
                           };

    my $component2 = MetaScheduler::Component->new({ %$component_pieces, 
						     task_id => 1
						   });

    print $component->dump();

    my @components = MetaScheduler::Component->find_components(1);

    print "Components: @components\n";
    my $cid = shift @components;

    my $component3 = MetaScheduler::Component->new({component_id => $cid });

    print $component3->dump();
};
