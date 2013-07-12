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
use MetaScheduler::Pipeline;
use MetaScheduler::Job;
use Scalar::Util 'reftype';
use Log::Log4perl;

my $alarm_timeout = 60;
my $error_backoff = 600;
my $max_errors = 6;

# Even though it's called "jobs" they will be
# MetaScheduler::Pipeline references, since Pipeline
# objects hold jobs

has 'jobs' => (
    traits  => ['Hash'],
    is      => 'rw',
    isa     => 'HashRef',
    default => sub { {} },
    handles => {
         set_job    => 'set',
         get_job    => 'get',
         delete_job => 'delete',
         clear_job  => 'clear',
         fetch_keys => 'keys',
    },
);

my $cfg; my $logger;
my $sig_int = 0;

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
	    $self->initializeScheduler($_);
#	    require "MetaScheduler/$_.pm";
#	    "MetaScheduler::$_"->initialize();
	}
    } else {
	my $scheduler = $cfg->{schedulers};
	{
#	    no strict 'refs';
	    $self->initializeScheduler($scheduler);
#	    require "MetaScheduler/$scheduler.pm";
#	    "MetaScheduler::$scheduler"->initialize();
	}
    }

    # Find all the running/pending jobs
    $self->initializeMetaScheduler();

}

# This is the work horse of the scheduler.
# Once the scheduler is setup and initialized
# this subroutine takes over and manages things.
# We have to do two main tasks, listen for connections
# on our TCP port from clients submitting or inquiring
# about jobs and cycle through existing jobs running 
# and managing them.

sub runScheduler {
    my $self = shift;

    # Setup TCP port

    # while we haven't received a finish signal
    while(!$sig_int) {

	# We're going to go through the jobs and 
	# deal with them one by one for this cycle
	for my $pipeline ($self->fetch_keys) {
	    $self->processJob($pipeline);
	}
    }
}

# For a pipeline, give it a time slice to do
# what it needs to, check if it's done, deal
# with any completion or errors.

sub processJob {
    my $self = shift;
    my $pipeline = shift;

    my $task_id = $pipeline->fetch_task_id;

    # Have we had errors in past attempts?
    if($pipeline->errors) {
	# Skipping, we'll provide a method to clear
	# error counters elsewhere via tcp
	return if($pipeline->errors > $max_errors);

	# How long are we supposed to wait before trying again?
	$wait_time = $pipeline->errors * $error_backoff;

	# Not long enough, skipping
	return if((time - $pipeline->last_run) < $wait_time);
    }

    # In an eval block to protect the scheduler
    eval {
	# Set an alarm so we don't get stuck
	# processing the pipeline
	local $SIG{ALRM} = sub { die "timeout\n" };
	alarm $alarm_timeout;
	$pipeline->run_iteration;
	alarm 0;
	# Set the last run time
	$pipeline->last_run = time;
    };
    # Did we get any errors back?
    if($@) {
	# Uh-oh, we had an alarm, the iteration timed out
	if($@ eq "timeout\n") {
	    $logger->error("Job timed out while running an interation, fail count " . $pipeline->errors . ", task_id $task_id");
	} else {
	    # Some other kind of error?
	    $logger->error("Error running iteration of task_id $task_id: " . $@);
	}
	# Count the error for our max error count and backoff
	$pipeline->errors++;
    }
}

# Callback function for TCP server to process
# inbound requests

sub process_request {
    my $self = shift;
    my $req = shift;


}

sub initializeMetaScheduler {
    my $self = shift;
    
    $self->loadJobs();

    # 
}

sub loadJobs {
    my $self = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;
    my $pipeline_base = $cfg->{pipelines};

    my $sqlstmt = qq{SELECT task_id, job_type FROM task WHERE run_status IN ('PENDING', 'HOLD', 'ERROR', 'RUNNING')};
    my $fetch_jobs = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $fetch_jobs->execute() or
	$logger->fatal("Error fetching jobs in initialization: $DBI::errstr");

    while(my @row = $fetch_jobs->fetchrow_array) {
	$logger->info("Initializing job $row[0] of type $row[1]");
	my $job; my $pipeline;
	eval {
	    $pipeline = MetaScheduler::Pipeline->new({pipeline => $pipeline_base . '/' . lc($row[1]) . '.config'});
	    $job = MetaScheduler::Job->new({task_id => $row[0]});
	    $pipeline->attach_job($job);
	    $pipeline->validate_state();
	    $pipeline->graph();
	};

	if($@) {
	    $logger->error("Error, can not initialize job $row[0] of type $row[1], skipping. [$@]");
	} else {
	    $logger->debug("Finished initializing job " . $self->concatName($job->job_type, $job->job_name) . ", saving.");
	    $self->set_job($self->concatName($job->job_type, $job->job_name) => $pipeline);
	}
    }
}

sub find_by_id {
    my $self = shift;
    my $task_id = shift;

    for my $pipeline ($self->fetch_keys) {
	my $job = $self->get_job($pipeline)->fetch_job;
	return $self->get_job($pipeline)
	    if($job->task_id == $task_id);
    }

    return undef;
}

sub concatName {
    my $self = shift;
    my $job_type = shift;
    my $job_name = shift;

    $job_type =~ s/\s/_/g;
    $job_name =~ s/\s/_/g;

    return $job_type . '_' . $job_name;
}

sub initializeScheduler {
    my $self = shift;
    my $scheduler = shift;

    eval {
	no strict 'refs';
	$logger->debug("Initializing scheduler MetaScheduler::$scheduler");
	require "MetaScheduler/$scheduler.pm";
	"MetaScheduler::$scheduler"->initialize();
    };

    if($@) {
	$logger->fatal("Error, can not load scheduler $scheduler: $@");
    }
}

sub fetchDefaultScheduler {
    my $self = shift;

    if(reftype $cfg->{schedulers} eq 'ARRAY') {
	return pop @{$cfg->{schedulers}};
    } else {
	return $cfg->{schedulers};
    }

}

sub finish {
    my $self = shift;

    $logger->info("Receiver terminate signal, exiting at the end of this cycle.");
    $sig_int = 1;
}

1;
