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
    },
);

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
	    $logger->debug("Finished initializing job, saving.");
	    $self->set_job($self->concatName($job->job_type, $job->job_name) => $job);
	}
    }
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

1;
