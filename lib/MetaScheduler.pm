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
use MetaScheduler::Server;
use MetaScheduler::Pipeline;
use MetaScheduler::Job;
use MetaScheduler::Authentication;
use MetaScheduler::ProcessReq;
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
         set_job     => 'set',
         get_job     => 'get',
         delete_job  => 'delete',
         clear_job   => 'clear',
         fetch_keys  => 'keys',
         fetch_values =>'values',
         job_pairs   => 'kv',
    },
);

has schedulers => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'ArrayRef[Ref]',
    default => sub { [] },
);

has 'queued_jobs' => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'ArrayRef[Ref]',
    default => sub { [] },
);

my $cfg; my $logger; my $server;
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

    # Initialize the logging
    my $log_cfg = $cfg->{'logger_conf'};
    die "Error, can't access logger_conf $log_cfg"
	unless(-f $log_cfg && -r $log_cfg);

    Log::Log4perl::init($log_cfg);
    $logger = Log::Log4perl->get_logger;

    # Initialize the TCP request handler class
    MetaScheduler::ProcessReq->initialize();

    # Initialize the authentication module
    MetaScheduler::Authentication->initialize;

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

    my $fastload = 0;
    if($args->{fastload}) {
	$logger->warn("Using fast load");
	$fastload = $args->{fastload};
    }

    # Find all the running/pending jobs
    $self->initializeMetaScheduler($fastload);

#    foreach my $k ($self->fetch_keys) {
#	my $p = $self->get_job($k);
#	print "$k: " . $p->fetch_task_id . " ". $p ."\n";
#    }
#    exit;

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

    $logger->info("Running the scheduler, start the loop!");

    # while we haven't received a finish signal
    while(!$sig_int) {

	$logger->trace("In the loop");
	$logger->info("Jobs being monitored: " . scalar($self->fetch_keys));

	if(my $delayed_job = shift @{ $self->queued_jobs }) {
	    $logger->warn("Delayed load of job [$delayed_job], loading...");
	    $self->reloadJob($delayed_job);
	}

	# We're going to go through the jobs and 
	# deal with them one by one for this cycle
	for my $name ($self->fetch_keys) {
	    my $pipeline = $self->get_job($name);
	    $logger->trace("Processing a pipeline $name, [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');

	    $self->processJob($pipeline, $name);

	    # We don't want to keep the tcp connections waiting,
	    # they're more of a priority
	    if($server->reqs_waiting(0.1)) {
		$server->process_requests($self);
	    }
	}

	# And do a gratuitous processing of requests
	# in case we have anything to send, and set the
	# timeout to 1 second so we're not spinning quite
	# as tight a loop.
	$logger->trace("Processing any TCP requests");
	$server->process_requests($self);
    }
}

# For a pipeline, give it a time slice to do
# what it needs to, check if it's done, deal
# with any completion or errors.

sub processJob {
    my $self = shift;
    my $pipeline = shift;
    my $name = shift;

    my $task_id = $pipeline->fetch_task_id;

    my $state = $pipeline->fetch_status;

    # Is the job finished?
    if($state eq 'COMPLETE') {
	# We keep jobs around for a fixed amount
	# of time, then remove them from memory to save
	# resources. If queried later they'll just have
	# to be pulled from disk
	my $timeout = $cfg->{cache_timeout} || 86400;
	if(time > ($pipeline->last_run + $timeout)) {
	    $logger->warn("Job has completed and expired, removing: $task_id [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');
	    $self->delete_job($name);
	}

	# Regardless, we don't need to process completed jobs
	return;
    }

    $logger->trace("Giving job $task_id a slice [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');

    # Have we had errors in past attempts?
    if($pipeline->errors) {
	# Skipping, we'll provide a method to clear
	# error counters elsewhere via tcp
	return if($pipeline->errors > $max_errors);

	# How long are we supposed to wait before trying again?
	my $wait_time = $pipeline->errors * $error_backoff;

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
	$pipeline->last_run(time);
    };
    # Did we get any errors back?
    if($@) {
	# Uh-oh, we had an alarm, the iteration timed out
	if($@ eq "timeout\n") {
	    $logger->error("Job timed out while running an interation, fail count " . $pipeline->errors . ", task_id $task_id [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');
	} else {
	    # Some other kind of error?
	    $logger->error("Error running iteration of task_id $task_id: [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . '] ' . $@);
	}
	# Count the error for our max error count and backoff
	$pipeline->inc_errors;
    } else {
	$pipeline->reset_errors;
    }
}

# Callback function for TCP server to process
# inbound requests

sub process_request {
    my $self = shift;
    my $req = shift;

    my ($ret_code, $ret_str) = MetaScheduler::ProcessReq->process_request($self, $req);

    return "$ret_str\n";
}

sub initializeMetaScheduler {
    my $self = shift;
    my $fastload = (@_ ? 1 : 0);
    
    # Initialize the TCP server
    MetaScheduler::Server->initialize();
    $server =  MetaScheduler::Server->instance;

    $self->loadJobs($fastload);

}

sub loadJobs {
    my $self = shift;
    my $fastload = (@_ ? 1 : 0);

    my $dbh = MetaScheduler::DBISingleton->dbh;
    my $pipeline_base = $cfg->{pipelines};

    my $sqlstmt = qq{SELECT task_id, job_type FROM task WHERE run_status IN ('PENDING', 'HOLD', 'ERROR', 'RUNNING')};
    my $fetch_jobs = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $fetch_jobs->execute() or
	$logger->fatal("Error fetching jobs in initialization: $DBI::errstr");

    while(my @row = $fetch_jobs->fetchrow_array) {
	$logger->info("Initializing job $row[0] of type $row[1]");
	my $job; my $pipeline;
	if($fastload) {
	    $logger->warn("Stashing away for loading later: task_id: $row[0], job_type: $row[1]");
	    push @{ $self->queued_jobs }, $row[0];
	    next;
	}

	eval {
	    $pipeline = MetaScheduler::Pipeline->new({pipeline => $pipeline_base . '/' . lc($row[1]) . '.config'});
	    $job = MetaScheduler::Job->new({task_id => $row[0]});
	    $pipeline->attach_job($job);
	    $pipeline->validate_state();
#	    $pipeline->graph();
	    $pipeline->last_run(time);
	};

	if($@) {
	    $logger->error("Error, can not initialize job $row[0] of type $row[1], skipping. [$@]");
	} else {
	    my $name = $self->concatName($job->job_type, $job->job_name);
	    $logger->trace("Finished initializing job $name, saving. [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');
	    $self->set_job($name => $pipeline);
	    $logger->debug("Task saved: " . $pipeline->fetch_task_id . ' ' . $pipeline->fetch_status . " [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');
	}

	# We don't want to keep the tcp connections waiting,
	# they're more of a priority
	if($server->reqs_waiting(0.1)) {
	    $server->process_requests($self);
	}

    }

#    foreach my $k ($self->fetch_keys) {
#	my $p = $self->get_job($k);
#	print "$k: " . $p->fetch_task_id . " ". $p ."\n";
#    }
#    print "\n";

#    foreach my $p (@job_ary) {
#	print $p->task_id . " ". $p ."\n";
#    }
#    print "\n";
}

# If we need to pull a complete & expired job back in to the scheduler
sub reloadJob {
    my $self = shift;
    my $task_id = shift;

    my $pipeline;
    my $dbh = MetaScheduler::DBISingleton->dbh;
    my $pipeline_base = $cfg->{pipelines};

    my $sqlstmt = qq{SELECT task_id, job_type FROM task WHERE task_id = ?};
    my $fetch_jobs = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $fetch_jobs->execute($task_id) or
	$logger->fatal("Error fetching jobs in initialization: $DBI::errstr");

    if(my @row = $fetch_jobs->fetchrow_array) {
	$logger->info("Reloading job $row[0] of type $row[1]");
	my $job;
	eval {
	    $pipeline = MetaScheduler::Pipeline->new({pipeline => $pipeline_base . '/' . lc($row[1]) . '.config'});
	    $job = MetaScheduler::Job->new({task_id => $row[0]});
	    $pipeline->attach_job($job);
	    $pipeline->validate_state();
#	    $pipeline->graph();
	    $pipeline->last_run(time);
	};

	if($@) {
	    $logger->error("Error, can not initialize job $row[0] of type $row[1], $@");
	    return 0;
	} else {
	    my $name = $self->concatName($job->job_type, $job->job_name);
	    $logger->trace("Finished initializing job $name, saving.");
	    $self->set_job($name => $pipeline);
	    $logger->debug("Task saved: " . $pipeline->fetch_task_id . ' ' . $pipeline->fetch_status . " [" . $pipeline->fetch_job_id . '], [' . $pipeline->fetch_job_name . ']');
	}
    } else {
	$logger->warn("Job $task_id wasn't found in the database");
	return 0;
    }

    return $pipeline;
}

sub addJob {
    my $self = shift;
    my $json = shift;

    my $pipeline_base = $cfg->{pipelines};
    my $job; my $pipeline;

    print "Adding job\n";
    print Dumper $json;

    eval {
	$pipeline = MetaScheduler::Pipeline->new({pipeline => $pipeline_base . '/' . lc($json->{job_type}) . '.config'});
	$job = MetaScheduler::Job->new({decoded_job => $json});
	$pipeline->attach_job($job);
	$pipeline->validate_state();
	$pipeline->graph();
    };
    if($@) {
	$logger->error("Error, can't add job: $@");
	return 0;
    } else {
	my $name = $self->concatName($job->job_type, $job->job_name);
	$logger->debug("Finished adding job $name, saving.");
	$self->set_job($name => $pipeline);
    }

    return $job->task_id;

}

# Return a json list of the jobs, limit by the
# job type if requested

sub showJob {
    my $self = shift;
    my $type = shift;

    my $json = '['; 
    my $records = 0;

    for my $name (sort $self->fetch_keys) {
	my $pipeline = $self->get_job($name);

	# Fetch the json from the pipeline
	my ($cur_json, $ret_type) = $pipeline->dump_json;

	# If we're limiting by type...
	next if($type && (lc($type) ne lc($ret_type)));

	# Add the record to the list
	$json .= ',' if($records > 0);
	$json .= $cur_json;
	$records++;
    }

    $json .= ']';

    return ($records, $json);
}

sub statusJob {
    my $self = shift;
    my $task_id = shift;

    my $pipeline = $self->find_by_id($task_id);

    # task_id doesn't exist
    return 0 unless($pipeline);

    return $pipeline->fetch_status;
}

sub alterJob {
    my $self = shift;
    my $task_id = shift;
    my $action = shift;
    my $component = shift;

    $logger->trace("Attempting to alter job $task_id to $action [component $component]");

    my $pipeline = $self->find_by_id($task_id);

    unless($pipeline) {
	$logger->warn("Task $task_id not found in memory, trying to fetch from DB");
	$pipeline = $self->reloadJob($task_id);

	unless($pipeline) {
	    $logger->error("Failed to reload task $task_id from DB, bailing");
	    return 0;
	}
    }

    my $res = 0;

    if($component) {
	$logger->debug("Updating $task_id, component $component to status $action");
	$res = $pipeline->set_state($action, $component);
    } else {
	$logger->debug("Updating $task_id to status $action");
	if(lc($action) eq 'hold') {
	    $res = $pipeline->set_state("HOLD");
	} elsif(lc($action) eq 'pending') {
	    $res = $pipeline->set_state("PENDING");
	} elsif(lc($action) eq 'delete') {
	    $res = $pipeline->set_state("DELETED");
	}
    }

    return $res;
}

sub resetJob {
    my $self = shift;
    my $task_id = shift;
 
   $logger->trace("Attempting to reset job $task_id");

    my $pipeline = $self->find_by_id($task_id);

    unless($pipeline) {
	$logger->warn("Task $task_id not found in memory, trying to fetch from DB");
	$pipeline = $self->reloadJob($task_id);

	unless($pipeline) {
	    $logger->error("Failed to reload task $task_id from DB, bailing");
	    return 0;
	}
    }

    $pipeline->reset_errors;

    return 1;
}

sub alterLogLevel {
    my $self = shift;
    my $level = shift;

    $logger->info("Adjusting logging level to $level");

    $logger->level($level);

    return 1;
}

sub refreshSchedulers {
    my $self = shift;

    $logger->info("Reloading schedulers");

    for my $s (@{$self->schedulers}) {
	eval {
	    $logger->trace("Refreshing scheduler $s");
	    no strict 'refs';
	    my $scheduler = "MetaScheduler::$s"->instance();
	    $scheduler->refresh(1);
	};
	if($@) {
	    $logger->error("Error refreshing scheduler $s: $@");
	}
    }

    return 1;
}

# We need a way to obsfucate the url, maybe
# it should be passed by an intermediary cgi,
# but that will be up to the client package
# using the api

sub graphJob {
    my $self = shift;
    my $task_id = shift;

    # Let's check the job exists as a sanity check
    # But should we?  If it's a really old job...
    my $pipeline = $self->find_by_id($task_id);

    # task_id doesn't exist
    return 0 unless($pipeline);

    # First check if the graph file exists, build the path
    my $graph_file = $cfg->{jobs_dir} . '/' . $task_id . '/' . $cfg->{graph_name};

    unless( -f $graph_file && -r $graph_file ) {
	$logger->error("Error, graph file $graph_file does not exist or isn't readable");
	return 0;
    }

    # Next build the url, which in apache should point at the same location!
    my $url = $cfg->{url_base};
    $url .= ($url =~ /\/$/ ? '' : '/');
    $url .= $task_id . '/' . $cfg->{graph_name};

    return $url;
}

# Return the pipeline object based on the task_id

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
	$logger->info("Initializing scheduler MetaScheduler::$scheduler");
	require "MetaScheduler/$scheduler.pm";
	"MetaScheduler::$scheduler"->initialize();

	# Remember the loaded schedulers
	push @{ $self->schedulers }, $scheduler;
    };

    if($@) {
	$logger->logdie("Error, can not load scheduler $scheduler: $@");
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

sub getCfg {
    my $self = shift;

    return $cfg;
}

sub finish {
    my $self = shift;

    $logger->info("Receiver terminate signal, exiting at the end of this cycle.");
    $sig_int = 1;
}

1;
