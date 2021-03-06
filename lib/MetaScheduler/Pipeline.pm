=head1 NAME

    MetaScheduler::Pipeline

=head1 DESCRIPTION

    Object for holding and managing individual pipelines
    It should have a job attached to it and this is the 
    main object for managing tasks in the scheduler.

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    March 27, 2013

=cut

package MetaScheduler::Pipeline;

use strict;
use warnings;
use JSON;
use Data::Dumper;
use Log::Log4perl;
use MetaScheduler::Config;
use Graph::Directed;
use Moose;
use GraphViz2;
#use Switch;
use feature qw{ switch };
use List::MoreUtils qw(uniq);

#my $pipeline;
#my $logger;
my $cfg;
#my $scheduler;
#my $g;
#my $job;
#my $last_run = 0;

has 'errors' => (
      traits  => ['Counter'],
      is      => 'ro',
      isa     => 'Int',
      default => 0,
      handles => {
          inc_errors   => 'inc',
          dec_errors   => 'dec',
          reset_errors => 'reset',
      },
);

has last_run => (
    is     => 'rw',
    isa    => 'Int'
);

sub BUILD {
    my $self = shift;
    my $args = shift;

    $self->{logger} = Log::Log4perl->get_logger unless($self->{logger});

    $cfg =  MetaScheduler::Config->config unless($cfg);

    die "Error, no pipeline file given" unless($args->{pipeline});

    $self->read_pipeline($args->{pipeline});

    $self->build_tree;
}

sub read_pipeline {
    my ($self, $pipeline_file) = @_;

    $self->{logger}->debug("Reading pipeline file $pipeline_file\n");

    die "Error, pipeline file not readable: $pipeline_file"
	unless ( -f $pipeline_file && -r $pipeline_file );

    my $json;
    {
	local $/; #enable slurp
	open my $fh, "<", "$pipeline_file";
	$json = <$fh>;
    } 

    eval {
	$self->{pipeline} = decode_json($json);
    };
    if($@) {
	# Error decoding JSON
	die "Error, can not decode pipeline file $pipeline_file: $!";
    }

}

sub build_tree {
    my ($self) = @_;

    $self->{logger}->debug("Building the graph for the pipeline");
    $self->{g} = Graph::Directed->new;

    foreach my $component (@{$self->{pipeline}->{'components'}}) {
	my $name = $component->{name};
	$self->{logger}->trace("Adding component $name");
	$self->add_edges($name, $component->{on_success}, 'success')
	    if($component->{on_success});

	$self->add_edges($name, $component->{on_failure}, 'failure')
	    if($component->{on_failure});

	# Save the meta information like status_test
	# to the node for referencing later
	$self->{g}->set_vertex_attribute($name, "status_test", $component->{status_test});
    }
    
    unless($self->{g}->is_dag()) {
	$self->{logger}->error("Error, pipeline for $self->{pipeline}->{job_type} isn't a directed acyclic graph");
	die "Error, pipeline for $self->{pipeline}->{job_type} isn't a directed acyclic graph";
    }

    if(my ($iv) = $self->{g}->isolated_vertices()) {
	# We'll allow issolated vertices if there's only one
	# component in the pipeline, of course it will be
	# isolated!
	unless($iv eq $self->{pipeline}->{'first_component'}) {
	    $self->{logger}->error("Error, pipeline for $self->{pipeline}->{job_type} has unreachable components");
	    die "Error, pipeline for $self->{pipeline}->{job_type} has unreachable component";
	}
    }

    print "Graph:\n$self->{g}\n";

}

# This will scan through an attached job
# and validate the states for components 
# ie. is a component really running as the database/object says
# It does this through using the status_test field
# It will update the state of a component if it finds the 
# listed state is incorrect

sub validate_state {
    my $self = shift;

    unless($self->{g}) {
	$self->{logger}->error("Error, no job seems to be attached to this task, can't validate");
	die "Error, no job seems to be attached to this task, can't validate";
    }

    vertex: foreach my $v ($self->{g}->vertices) {
	my $c = $self->{job}->find_component($v);

	$self->{logger}->trace("Evaluating state for task " . $self->{job}->task_id . " component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

	unless($c) {
	    $self->{logger}->error("Error, we can't find component $v in job [" . $self->{job}->task_id . "], [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    next vertex;
	}

	# Fetch the status we think the job is
	my $state = $self->{job}->find_component_state($v);
	$self->{logger}->trace("Component $v for task [" . $self->{job}->task_id . "] thinks it is $state, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

	# We only care if we think it's running
	next vertex unless($state eq 'RUNNING');
	$self->{logger}->trace("Component $v for task [" . $self->{job}->task_id . "] is RUNNING, validating, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

	# Check the scheduler to see if the job is there
	my $sched_state = $self->{scheduler}->fetch_job_state($c->qsub_id);
	$self->{logger}->debug("Validating if component is really running, scheduler says component $v is in state $sched_state, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

	# The Torque scheduler module will never return a ERROR
	# state, but this switch statement might have to be
	# revisted if we add other schedulers in the future
	# that could return ERROR
	my $test_state;
	given($sched_state) {
	    # This means it's no longer in the scheduler
	    # we need to depend on the status_test to see
	    # what happened
	    when ("UNKNOWN")    { $test_state = $self->confirm_state($v); }
	    # It's in the scheduler waiting for something,
	    # in this context treat this as running,
	    # we're just waiting
	    when ("HOLD")       { next vertex; }
	    # It's done, now we need to use the status_test
	    # to see what happened
	    when ("COMPLETE")   { $test_state = $self->confirm_state($v); }
	    # In this context treat this as running since
	    # we're simply waiting
	    when ("PENDING")    { next vertex; }
	    # If it's happily running we'll leave it be
	    when ("RUNNING")    { next vertex; }
	}

	# Now we have to deal with jobs that have exited the scheduler
	# and what their current state actually is, did it succeed,
	# fail, or die silently?  If the scheduler says COMPLETE
	# but confirm_status says RUNNING, it died silently, error!
	$self->{logger}->debug("Setting state for job [" . $self->{job}->task_id . "], component $v to $test_state, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

	given($test_state) {
	    when ("TIMEOUT")    { $self->{logger}->info("Temporary timeout when checking component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']'); 
	    }
	    when ("PENDING")    { $self->{logger}->warn("We're still in pending for some reason, did it fail to start component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
				  $self->{job}->change_state({component_type => $v,
						      state => 'PENDING' });
	    }
	    when ("COMPLETE")   { $self->{job}->change_state({component_type => $v,
						      state => 'COMPLETE' }); 
				  $self->send_mail();
	    }
	    when ("ERROR")      { $self->{job}->change_state({component_type => $v,
						      state => 'ERROR' }); 
				  $self->send_mail();
	    }
	    when ("RUNNING")    { $self->{job}->change_state({component_type => $v,
						      state => 'ERROR' }); }
	}
    }

}

# For an individual component, confirm the state
# using the status_test script. Update it's status as needed

sub confirm_state {
    my $self = shift;
    my $c = shift;

    return undef unless($self->{g}->has_vertex($c));

    my $cmd = $self->{g}->get_vertex_attribute($c, 'status_test');


    $cmd =~ s/\%\%jobid\%\%/$self->{job}->job_id/e;
    $cmd =~ s/\%\%component\%\%/$c/e;
    $self->{logger}->trace("Running command $cmd to check state of job [" . $self->{job}->task_id . "], [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    my $ret = system($cmd);
    my $rv = $? >> 8;

    $self->{logger}->trace("Received return value $rv, ret: $ret [" . $self->{job}->task_id . "], [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    if($rv == -1) {
	$self->{logger}->error("Failed to execute $cmd for job [" . $self->{job}->task_id . "], [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return "ERROR";
       
    } elsif($rv & 2) {
	$self->{logger}->trace("Task [" . $self->{job}->task_id . "] appears to be pending, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return "PENDING";
    } elsif($rv & 4) {
	$self->{logger}->trace("Task [" . $self->{job}->task_id . "] completed with an error, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return "ERROR";
    } elsif($rv & 8) {
	$self->{logger}->trace("Task [" . $self->{job}->task_id . "] seems to still be running, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return "RUNNING";
    } elsif($rv & 16) {
	$self->{logger}->trace("Task [" . $self->{job}->task_id . "] had a temporary TIMEOUT, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return "TIMEOUT";
    }

    $self->{logger}->trace("Task [" . $self->{job}->task_id . "] seems to have completed successfully, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
    return "COMPLETE";

}

sub run_component {
    my $self = shift;
    my $ctype = shift;

    # We can't run on a pipeline with no job attached
    return undef unless($self->{job} && $self->{g});

    my $c = $self->{job}->find_component($ctype);
    unless($c) {
	$self->{logger}->error("Component $ctype not found in job [" . $self->{job}->task_id . "] can't start, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return undef;
    }

    $self->{logger}->debug("Starting component $ctype for job [" . $self->{job}->task_id ."], [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
    
    # Send the start request to the scheduler object
    # Name it with the task_id+component_type
    my $sched_id = $self->{scheduler}->submit_job($self->{job}->task_id . "_$ctype", $c->qsub_file, "$cfg->{jobs_dir}/" . $self->{job}->task_id);

    # Did the task submit successfully?
    if($sched_id > 0) {
	# Update the assigned scheduler task number in the component
	$self->{job}->change_state({state => "RUNNING",
			    component_type => $ctype,
			    qsub_id => $sched_id
			   });

	# And make sure we're actually running!
#	$self->{job}->change_state({state => 'RUNNING'});
    } elsif($sched_id < 0) {
	$self->{logger}->info("The scheduler appears to be full, deferring job, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
    } else {
	# We couldn't submit the job to the scheduler, hold the job for review
	$self->{logger}->error("Error, can not submit component $ctype to scheduler for job [" . $self->{job}->task_id . "], holding job, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	$self->{job}->change_state({state => "HOLD"});
    }
}

sub add_edges {
    my ($self, $origin, $vertices, $label) = @_;

    $self->{logger}->debug("Adding edges for component $origin, label $label, verticies $vertices");

    foreach my $d (split ',', $vertices) {
	$self->{g}->add_edge($origin, $d);
	$self->{g}->set_edge_attribute($origin, $d, $label, 1);
    }
}

sub find_entry_points {
    my $self = shift;

    my @v = $self->{g}->predecessorless_vertices();

#    print "entry points\n";
#    print Dumper @v;
    return @v;
}

sub attach_job {
    my $self = shift;
    $self->{job} = shift;

    $self->overlay_job;

    my $sched = $self->{job}->job_scheduler;
    # will this work?
    # $self->{scheduler} = "MetaScheduler::$job->job_scheduler"->instance();
    eval {
	no strict 'refs';
	$self->{scheduler} = "MetaScheduler::$sched"->instance();
    }
}

# We call this each time the scheduler wants to give the
# job a turn to run a step

sub run_iteration {
    my $self = shift;

    # We can't run an iteration if there's no job attached
    unless($self->{job} && $self->{g}) {
	$self->{logger}->error("Error, can't run an iteration on pipeline, no job attached");
	die "Error, can't run an iteration on pipeline, nob job attached";
    }

    $self->{logger}->trace("Running iteration of job " . $self->{job}->job_type . " with job_id [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    # Before we begin an iteration, validate the state of the job
    $self->validate_state();

    # Next we clean up the graph for any state changes that occurred in this
    # iteration
    my @starts = $self->find_entry_points;

    # Walk the graph and remove edges for completed/errored jobs
    foreach my $start (@starts) {
	my $v = $start;

	$self->overlay_walk_component($v);
    }

    # We wanted to clean up finished components, but if the job
    # itself is HOLD, don't run anything, we're done
    return if($self->{job}->run_status eq 'HOLD');

    # Now go through again and find the components that are pending
    # and all their parents are complete, following the success/failure
    # path
    $self->{logger}->trace("Walking the job looking for components to run, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
    foreach my $start (@starts) {
	$self->{logger}->trace("Checking $start, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	my $v = $start;

	$self->walk_and_run($v)
    }

    # And before we relinquish control let's verify our state, so
    # the scheduler knows what state we're in
    $self->update_job_status;

}

# Walk the graph and run the components that are pending if
# all their parents are complete or error.  We don't need to
# worry about which path to take because before this function 
# is called overlay_walk_component should have been called to
# clean up the paths depending on success/failures of components
# All we have to do is simply follow the paths available 
# recursively.

sub walk_and_run {
    my $self = shift;
    my $v = shift;

    $self->{logger}->trace("Walking component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    my $state = $self->{job}->find_component_state($v);

    given($self->{job}->find_component_state($v)) {
	when ("PENDING")    { 
	    # Unless all the predecessors are complete or error
	    # we can't run this component
	    $self->{logger}->trace("Component $v is pending, checking parents, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
#	    foreach my $u ($self->{g}->predecessors($v)) {
#		my $s = $job->find_component_state($u);
#		return unless(($s eq 'COMPLETE') || ($s eq 'ERROR'));
#	    }

	    return unless($self->is_runable($v));

	    $self->{logger}->trace("Looks good for $v, trying to run, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    $self->run_component($v);
	    return;
	}
	when ("COMPLETE")   { 
	    $self->{logger}->trace("Component $v complete, walking children, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    foreach my $u ($self->{g}->successors($v)) {
		$self->walk_and_run($u);
	    }
	}
	when ("HOLD")       { return; }
	when ("ERROR")      { 
	    $self->{logger}->trace("Component $v error, walking children, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    foreach my $u ($self->{g}->successors($v)) {
		$self->{logger}->trace("Child of $v is $u [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
		$self->walk_and_run($u);
	    }
	}
	when ("RUNNING")    { return; }
    }
    
}

sub is_runable {
    my $self = shift;
    my $v = shift;

    return 0 unless($self->{job}->find_component_state($v) eq "PENDING");

    foreach my $u ($self->{g}->predecessors($v)) {
	my $s = $self->{job}->find_component_state($u);
	return 0 unless(($s eq 'COMPLETE') || ($s eq 'ERROR'));
    }

    return 1;
}

# Fetch the job component of the task

sub fetch_job {
    my $self = shift;

    return $self->{job} if($self->{job});

    return undef;
}

# Shortcut to fetch the task_id

sub fetch_task_id {
    my $self = shift;

    return $self->{job}->task_id if($self->{job});

    return 0;
}

# This might not catch all the corner cases, but
# if there is anything running, well, it's running.
# Else if there is anything held, that needs to be
# examined and the status is hold.
# Only three possibilities left, complete, error or
# pending.
# Pending is only if all source verticies are pending,
# otherwise we've begun.
# If all the sink verticies are complete/error, we're done.
# The only other two options are we're running but we have
# items not let in the queue, but if we've begun and can't
# actually run anything we're stuck, that's bad.

sub update_job_status {
    my $self = shift;

    my $states = $self->{job}->find_all_state;

    # If the job is in HOLD, it's been placed there
    # for a reason, don't alter it.
    return
	if($self->{job}->run_status eq 'HOLD');

    # If the job is in ERROR, something bad must have
    # happened, don't second guess ourself.
    return
	if($self->{job}->run_status eq 'ERROR');

    # If the job is in DELETED, don't try and run any more components,
    # though we're welcome to finish running existing components
    return
	if($self->{job}->run_status eq 'DELETED');

    if($states->{"RUNNING"}) {
	$self->{job}->run_status("RUNNING");
	return;
    } elsif($states->{"HOLD"}) {
	$self->{job}->run_status("HOLD");
	return;
    }

    my @starts = $self->find_entry_points();
    my $started = 0;
    foreach my $v (@starts) {
	$started = 1 unless($self->{job}->find_component_state($v) eq "PENDING");
    }
    unless($started) {
	$self->{job}->run_status("PENDING");
	return;
    }

    my $ended = 1;
    my $error = 0;
    foreach my $v ($self->{g}->sink_vertices()) {
	my $s = $self->{job}->find_component_state($v);
	$ended = 0 unless($s eq 'COMPLETE' ||
			  $s eq 'ERROR');
	$error = 1 if($s eq 'ERROR');
    }
    if($ended) {
	if($error) {
	    $self->{job}->run_status('ERROR');
	}
	$self->{job}->run_status('COMPLETE');
	return;
    }

    # Now we're in to the weird states, either stuck or
    # just nothing happens to be in the queue
    $self->{logger}->error("We seem to be stuck in this job, that's not good. Job: [" . $self->{job}->task_id . '], [' . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

}

# Return an array of all runable components
# Runable means all their parents are complete or
# error (or it's a start point)

sub find_runable {
    my $self = shift;
    my $v = shift;

    return unless($self->{job} && $self->{g});

    my $runable;
    $self->{logger}->trace("Finding runable components for pipeline, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    unless($v) {
	my @starts = $self->find_entry_points;

	foreach my $start (@starts) {
	    $self->{logger}->trace("Checking start point $start, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    if($self->is_runable($start)) {
		push @{$runable}, $start;
		$self->{logger}->debug("Start point $start is runable, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
		next;
	    }

	    foreach my $u ($self->{g}->successors($start)) {
		push @{$runable}, $self->find_runable($u);
	    }

	}

#	@{$runable} = grep { ! $seen{ $_ }++ } @{$runable};
	@{$runable} = uniq(@{$runable});
	return $runable;
    }

    $self->{logger}->trace("Checking state for $v");
    my $state = $self->{job}->find_component_state($v);

    given($self->{job}->find_component_state($v)) {
	when ("PENDING")    { 
	    # Unless all the predecessors are complete or error
	    # we can't run this component
	    $self->{logger}->trace("Component $v is pending, checking parents, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
#	    foreach my $u ($g->predecessors($v)) {
#		my $s = $job->find_component_state($u);
#		return unless(($s eq 'COMPLETE') || ($s eq 'ERROR'));
#	    }

	    return unless($self->is_runable($v));

	    $self->{logger}->debug("Looks good for $v, is runable, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    return $v;
	}
	when ("COMPLETE")   { 
	    $self->{logger}->debug("Component $v complete, walking children, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    foreach my $u ($self->{g}->successors($v)) {
		push @{$runable}, $self->find_runable($u);
	    }
	    ($runable ? return @{$runable} : return );
	}
	when ("HOLD")       { return; }
	when ("ERROR")      { 
	    $self->{logger}->debug("Component $v error, walking children, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    foreach my $u ($self->{g}->successors($v)) {
		push @{$runable}, $self->find_runable($u);
	    }
	    ($runable ? return @{$runable} : return );
	}
	when ("RUNNING")    { return; }
    }
 
}

sub overlay_job {
    my $self = shift;

# I don't rememberwhy we wouldn't want to overlay the job
# in other states, seems silly, we need to overlay
# pending jobs if we're adding them new
#    unless(($self->{job}->run_status eq "RUNNING") ||
#	   ($self->{job}->run_status eq "ERROR") ||
#	   ($self->{job}->run_status eq "COMPLETE")) {

#	$self->{logger}->info("Job is in state " . $self->{job}->run_status . ", not overlaying over pipeline");
#	return;
#    }

    my @starts = $self->find_entry_points;

    # Walk the graph and remove edges for completed/errored jobs
    foreach my $start (@starts) {
	my $v = $start;

	$self->overlay_walk_component($v);
    }
    
}

sub overlay_walk_component {
    my $self = shift;
    my $v = shift;

    $self->{logger}->debug("Walking component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    # Is is a sink vertex, or a vertex with no children
    if($self->{g}->is_sink_vertex($v)) {
	$self->{logger}->trace("Vertex $v is a sink, stopping, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	return;
    } else {
	my $state = $self->{job}->find_component_state($v);

	if($state eq "COMPLETE") {
	    # Job is complete, we remove the on_failure edges
	    # and continue walking the on_success edges
	    $self->remove_edges($v, "failure");
	} elsif($state eq "ERROR") {
	    # Job failed, we remove the on_success edges
	    # and continue to walk the on_failure edges
	    $self->remove_edges($v, "success");
	} elsif($state eq "PENDING") {
	    # Hasn't started yet, we stop here because we
	    # don't know which direction to go
	    $self->{logger}->trace("Component $v is PENDING, no where to go, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    return;

	} elsif($state eq "HOLD") {
	    # Component is on hold, we stop here because we
	    # don't know which direction to go
	    $self->{logger}->trace("Component $v is HOLD, no where to go, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    return;

	} elsif($state eq "RUNNING") {
	    # Component is running, we stop here because we
	    # don't know which direction to go
	    $self->{logger}->trace("Component $v is RUNNING, no where to go, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    return;

	} else {
	    $self->{logger}->error("Unknown state for component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    die "Error, unknown state for component $v, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']';
	}

	# Recursively walk the remaining edges, if any
	foreach my $u ($self->{g}->successors($v)) {
	    $self->overlay_walk_component($u);
	}
    }
}

sub remove_edges {
    my $self = shift;
    my $v = shift;
    my $attr = shift;

    foreach my $u ($self->{g}->successors($v)) {

	if($self->{g}->has_edge_attribute($v, $u, $attr)) {
	    $self->{logger}->debug("Removing attribute from edge $v, $u: $attr, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    $self->{g}->delete_edge_attribute($v, $u, $attr);
	    unless($self->{g}->has_edge_attributes($v, $u)) {
		$self->{logger}->debug("Removing edge $v, $u with value $attr, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
		$self->{g}->delete_edge($v, $u);
		$self->scrub_dangling_vertices($u);
	    } else {
		$self->{logger}->debug("Multiple attributes on edge $v, $u, not deleting, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
	    }
	}
    }
}

# When we walk the graph and remove an edge
# we have to clean up in case it leaves a 
# dangling start point (a vertex with no parents),
# this would get caught in our next scan for start points

sub scrub_dangling_vertices {
    my $self = shift;
    my $v = shift;

    return unless($self->{g}->is_predecessorless_vertex($v));

    my @successors = $self->{g}->successors($v);

    $self->{logger}->debug("Vertex $v has no parents, removing, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');
    $self->{g}->delete_vertex($v);

    foreach my $u (@successors) {
	$self->scrub_dangling_vertices($u);
    }
}

sub fetch_component {
    my ($self, $name) = @_;

    die "Error, no pipeline loaded"
	unless($self->{pipeline});

    $self->{logger}->trace("Returning pipeline component $name, [" . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']');

    foreach my $component (@{$self->{pipeline}->{'components'}}) {
	return $component
	    if($name eq $component->{'name'});
    }

    return 0;
}

sub fetch_status {
    my $self = shift;

    if($self->{job}) {
	return $self->{job}->run_status;
    }

    return undef;
}

sub fetch_job_id {
    my $self = shift;

     if($self->{job}) {
	return $self->{job}->job_id;
    }

    return undef;
}

sub fetch_job_name {
    my $self = shift;

     if($self->{job}) {
	return $self->{job}->job_name;
    }

    return undef;
}

sub set_state {
    my $self = shift;
    my $state = shift;
    my $component = shift;

    # If the job doesn't exist
    return 0 unless($self->{job});

    if($component) {
	$self->{job}->change_state({state => $state, component_type => $component});
    } else {
	if(uc($state) eq "HOLD") {
	    $self->{job}->change_state({state => "HOLD"});
	} elsif(uc($state) eq "PENDING") {
	    $self->{job}->change_state({state => "PENDING"});
	    $self->reset_errors;
	} elsif(uc($state) eq "DELETED") {
	    $self->{job}->change_state({state => "DELETED"});
	} else {
	    return 0;
	}
    }

    return 1;

}

sub send_mail {
    my $self = shift;

    $self->{logger}->error("Error, no mailer_script defined, can't send mail for [" . $self->{job}->task_id . '], [' . $self->{job}->job_id . '], [' . $self->{job}->job_name . ']')
	unless($self->{pipeline}->{mailer_script});

    # Get the script specified and substitute in the values
    my $cmd = $self->{pipeline}->{mailer_script};
    $cmd =~ s/\%\%jobid\%\%/$self->{job}->job_id/e;
    $cmd =~ s/\%\%status\%\%/$self->{job}->run_status/e;

    # Just run it
    `$cmd`;

}

sub graph {
    my $self = shift;
    my $fields = shift;

    # We can do a graph unless we have a job attached
    return unless($self->{job});

    my $gv = GraphViz2->new(global => {directed => 1});

    foreach my $v ($self->{g}->vertices) {
	$self->graph_node($gv, $v, $fields);
#	$gv->add_node(name => $v);
    }

    foreach my $e ($self->{g}->edges) {
	if($self->{g}->has_edge_attribute($e->[0], $e->[1], 'success') && $self->{g}->has_edge_attribute($e->[0], $e->[1], 'failure')) {
	    $gv->add_edge(from => $e->[0], to => $e->[1]);
	} elsif($self->{g}->has_edge_attribute($e->[0], $e->[1], 'success')) {
	    $gv->add_edge(from => $e->[0], to => $e->[1], color => 'green', label => 'on success');
	} elsif($self->{g}->has_edge_attribute($e->[0], $e->[1], 'failure')) {
	    $gv->add_edge(from => $e->[0], to => $e->[1], color => 'red', label => 'on failure');
	}
    }

    $gv->run(format => 'svg', output_file => $cfg->{jobs_dir} . '/' . $self->{job}->task_id . '/' . $cfg->{graph_name});
}

sub graph_node {
    my $self = shift;
    my $gv = shift;
    my $v = shift;
    my $fields = shift;

    my $c;

    return unless($c = $self->{job}->find_component($v));

#    print Dumper MetaScheduler::Component->meta->get_attribute_list;

    my $colour;
    given($c->run_status) {
	when ("PENDING")   {$colour = "yellow"}
	when ("COMPLETE")  {$colour = "grey"}
	when ("HOLD")      {$colour = "blue"}
	when ("ERROR")     {$colour = "red"}
	when ("RUNNING")   {$colour = "green"}
    }

    my $label = "<<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\"><TR><TD PORT=\"f0\" bgcolor=\"$colour\"><B>$v</B></TD></TR>";
    my $port = 1;
    foreach my $field (@{$cfg->{graph_fields}}) {
	if($field =~ /date$/) {
	    next unless($c->$field);
	    $label .= "<TR><TD PORT=\"f$port\">$field: ". scalar localtime($c->$field) . "</TD></TR>";
	} else {
	    $label .= "<TR><TD PORT=\"f$port\">$field: ". $c->$field . "</TD></TR>";
	}
	$port++;
    }
    $label .= "</TABLE>>";
    $gv->add_node(name => $v, shape => 'record', label => $label);

#    $gv->add_node(name => $v, shape => 'record', color => $colour, label => [$v, join('\l', map{$i++; "$_ $i : " . $c->$_} MetaScheduler::Component->meta->get_attribute_list) . '\l']);
#    my $i = 1;
#    $gv->add_node(name => $v, shape => 'record', color => $colour, label => "<<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\"><TR><TD PORT=\"f1\" bgcolor=\"$colour\"><B>title</B></TD></TR><TR><TD PORT=\"f2\">index</TD></TR><TR><TD PORT=\"f3\">field1</TD></TR><TR><TD PORT=\"f4\">field2</TD></TR></TABLE>>");
#$v, join('\l', map{$i++; "$_ $i : " . $c->$_} MetaScheduler::Component->meta->get_attribute_list) . '\l']);
#    $gv->add_node(name => $v, shape => 'record', color => $colour, label => [$v, $c->run_status]);

    
}

sub dump_json {
    my $self = shift;

    my $job = $self->fetch_job;

    # This should never happen, unless someone does something
    # silly like call the dump routine on a pipeline with no
    # job attached, which isn't really an error, there's just
    # nothing to report
    return unless($job);

    my $json_data;

    $json_data->{job_name} = $job->job_name;
    $json_data->{job_type} = $job->job_type;
    $json_data->{task_id} = $job->task_id;
    $json_data->{run_status} = $job->run_status;
    $json_data->{extra_parameters} = $job->extra_parameters;
    $json_data->{priority} = $job->priority;
    $json_data->{job_scheduler} = $job->job_scheduler;
    $json_data->{submitted_date} = $job->submitted_date;
    $json_data->{start_date} = $job->start_date;
    $json_data->{complete_date} = $job->complete_date;
    $json_data->{components} = $job->component_structs;

    my $json = to_json($json_data, {pretty => 1});

    return ($json, $job->job_type);

#    my $json = "{\n";
#    $json .= " \"job_name\": \"" . $job->job_name . "\",\n";
#    $json .= " \"job_type\": \"" . $job->job_type . "\",\n";
#    $json .= " \"task_id\": \""  . $job->task_id  . "\",\n";
#    $json .= " \"run_status\": \"" . $job->run_status . "\",\n";
#    $json .= " \"extra_parameters\": \"" . $job->extra_parameters . "\",\n";
#    $json .= " \"priority\": \"" . $job->priority . "\",\n";
#    $json .= " \"job_scheduler\": \"" . $job->job_scheduler . "\",\n";
#    $json .= " \"submitted_date\": \"" . $job->submitted_date . "\",\n";
#    $json .= " \"start_date\": \"" . $job->start_date . "\",\n";
#    $json .= " \"complete_date\": \"" . $job->complete_date . "\",\n";
#    $json .= " \"components\": " . $job->dump_components_json("\t") . "\n";
#    $json .= "}";

    return ($json, $job->job_type);
}

sub dump_graph {
    my $self = shift;

    print "Graph $self->{g}\n";
}

1;
