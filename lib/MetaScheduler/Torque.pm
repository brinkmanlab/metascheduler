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

=todo

Purge completed jobs older than x hours/days?
Job counter, qstat should fill in stats about # of jobs in each running
state, function to say is there any spare room to submit a new job
according to the max jobs in the config file, and when submitting a job the
running job counter should increment.

=cut

package MetaScheduler::Torque;

use strict;
use warnings;
use Log::Log4perl;
use MooseX::Singleton;
use MetaScheduler::Torque::QStat;
use MetaScheduler::Torque::QSub;
use feature qw{ switch };
use Data::Dumper;

my $logger;

sub initialize {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

    MetaScheduler::Torque::QStat->initialize;
    MetaScheduler::Torque::QSub->initialize();
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

# Submit a job to the scheduler, return
# the scheduler id (qsub_id), -1 if submission failed,
# 0 if there's no additional job capacity available

sub submit_job {
	my $self = shift;
	my $name = shift;
	my $qsub_file = shift;
	my $job_dir = shift;
	
	return MetaScheduler::Torque::QSub->submit_job($name, $qsub_file, $job_dir);
}

# See if there's room for more jobs

sub scheduler_full {
    my $self = shift;

    return MetaScheduler::Torque::QSub->scheduler_full();
}

1;
