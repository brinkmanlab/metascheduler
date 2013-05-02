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
use MetaScheduler::DBISingleton;
use MetaScheduler::Mailer;

MAIN: {
    my $cfname;
    my $res = GetOptions("config=s" => \$cfname
    );

    die "Error, no config file given"
      unless($cfname);

    my $metascheduler = MetaScheduler->new({cfg_file => $cfname });

    my @emails = ('lairdm@sfu.ca', 'matt@luther.ca');

    my $mailer = MetaScheduler::Mailer->new({task_id => 1, emails => \@emails });

    my $emails = $mailer->fetch_emails;
    print Dumper $emails;

    $mailer->send_email({ from => 'metascheduler-mailer@brinkman.sfu.ca', 
			  subject => "Test email",
			  msg => "And we've finished the job",
			  resend => 0,
			});

    $emails = $mailer->fetch_emails;
    print Dumper $emails;

    $mailer = MetaScheduler::Mailer->new({task_id => 1});
    $emails = $mailer->fetch_emails;
    print Dumper $emails;

    $mailer->send_email({ from => 'metascheduler-mailer@brinkman.sfu.ca', 
			  subject => "Test email",
			  msg => "And we've finished the job",
			  resend => 0,
			});
}
