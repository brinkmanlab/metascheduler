=head1 NAME

    MetaScheduler::Torque

=head1 DESCRIPTION

    Interface to Torque/PBS

    Not invoked on it's own, an internal object of Pipeline

    Maybe should be subclassed from something more general
    to create a standardized API to different schedulers? (bugaboo?)

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    April 4, 2013

=cut

package MetaScheduler::Torque;

use strict;
use warnings;
use Log::Log4perl;
use MooseX::Singleton;
use MetaScheduler::Torque::QStat;
use feature qw{ switch };
use Data::Dumper;

my $logger;

sub initialize {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

    MetaScheduler::Torque::QStat->initialize;
}

sub instance {
    my $self = shift;

    return $self;
}

sub fetch_job {
    my $self = shift;
    my $job_id = shift;

    return MetaScheduler::Torque::QStat->fetch($job_id);
}

sub fetch_job_state {
    my $self = shift;
    my $job_id = shift;

    my $job = MetaScheduler::Torque::QStat->fetch($job_id);

    return 'UNKNOWN' unless($job);

    given($job->{job_state}) {
	when ("E")    { return "COMPLETE" }
	when ("H")    { return "HOLD" }
	when ("Q")    { return "PENDING" }
	when ("R")    { return "RUNNING" }
	when ("T")    { return "RUNNING" }
	when ("W")    { return "PENDING" }
	when ("S")    { return "HOLD" }
    }
}

1;
