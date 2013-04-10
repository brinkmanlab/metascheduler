=head1 NAME

    MetaScheduler

=head1 DESCRIPTION

    Object for holding and managing emails for jobs

=head1 SYNOPSIS

    use MetaScheduler;

    

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    April 2, 2013

=cut

package MetaScheduler;

use strict;
use warnings;
use Moose;
use Data::Dumper;
use MetaScheduler::DBISingleton;
use MetaScheduler::Config;
#use MetaScheduler::Torque;
#use MetaScheduler::Torque::QStat;
use Scalar::Util 'reftype';
use Log::Log4perl;

my $cfg; my $logger;

sub BUILD {
    my $self = shift;
    my $args = shift;

    # Initialize the configuration file
    MetaScheduler::Config->initialize({cfg_file => $args->{cfg_file} });
    $cfg =  MetaScheduler::Config->config;

    # Initialize the DB connection
    MetaScheduler::DBISingleton->initialize({dsn => $cfg->{'dsn'}, 
					     user => $cfg->{'dbuser'}, 
					     pass => $cfg->{'dbpass'} });

    my $log_cfg = $cfg->{'logger_conf'};
    die "Error, can't access logger_conf $log_cfg"
	unless(-f $log_cfg && -r $log_cfg);

    Log::Log4perl::init($log_cfg);
    $logger = Log::Log4perl->get_logger;

    # Initialize the schedulers we're using
    if(reftype $cfg->{schedulers} eq 'ARRAY') {
	foreach (@{$cfg->{schedulers}}) {
	    require "MetaScheduler/$_.pm";
	    "MetaScheduler::$_"->initialize();
	}
    } else {
	my $scheduler = $cfg->{schedulers};
	{
	    no strict 'refs';
	    require "MetaScheduler/$scheduler.pm";
	    "MetaScheduler::$scheduler"->initialize();
	}
    }

#    MetaScheduler::Torque->initialize();

}

1;
