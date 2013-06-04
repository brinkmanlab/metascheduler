package MetaScheduler::Torque::QSub;

use strict;
use warnings;
use Log::Log4perl;
use MooseX::Singleton;
use MetaScheduler::Config;

my $logger;

sub initialize {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

}

# Submit a job to the scheduler, return
# the scheduler id (qsub_id), -1 if submission failed

sub submit_job {
	my $self = shift;
	my $name = shift;
	my $qsub_file = shift;
	
    my $config = MetaScheduler::Config->config;
	
	my $cmd = $config->{torque_qsub};
	
	# Prefix job name with MetaScheduler_
	# so we can fish them out in QStat
}

1;
