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

my $pipeline;
my $logger;
my $cfg;
my $scheduler;
my $g;
my $job;
my $errors = 0;
my $last_run = 0;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger;

    $cfg =  MetaScheduler::Config->config;

    die "Error, no pipeline file given" unless($args->{pipeline});

    $self->read_pipeline($args->{pipeline});

    $self->build_tree;
}

sub read_pipeline {
    my ($self, $pipeline_file) = @_;

    $logger->debug("Reading pipeline file $pipeline_file\n");

    die "Error, pipeline file not readable: $pipeline_file"
	unless ( -f $pipeline_file && -r $pipeline_file );

    my $json;
    {
	local $/; #enable slurp
	open my $fh, "<", "$pipeline_file";
	$json = <$fh>;
    } 

    eval {
	$pipeline = decode_json($json);
    };
    if($@) {
	# Error decoding JSON
	die "Error, can not decode pipeline file $pipeline_file: $!";
    }

}

sub build_tree {
    my ($self) = @_;

    $logger->debug("Building the graph for the pipeline");
    $g = Graph::Directed->new;

    foreach my $component (@{$pipeline->{'components'}}) {
	my $name = $component->{name};
	$logger->debug("Adding component $name");
	$self->add_edges($name, $component->{on_success}, 'success')
	    if($component->{on_success});

	$self->add_edges($name, $component->{on_failure}, 'failure')
	    if($component->{on_failure});

	# Save the meta information like status_test
	# to the node for referencing later
	$g->set_vertex_attribute($name, "status_test", $component->{status_test});
    }
    
    unless($g->is_dag()) {
	$logger->error("Error, pipeline for $pipeline->{job_type} isn't a directed acyclic graph");
	die "Error, pipeline for $pipeline->{job_type} isn't a directed acyclic graph";
    }

    if($g->isolated_vertices()) {
	$logger->error("Error, pipeline for $pipeline->{job_type} has unreachable components");
	die "Error, pipeline for $pipeline->{job_type} has unreachable component";
    }

    print "Graph:\n$g\n";

}

# This will scan through an attached job
# and validate the states for components 
# ie. is a component really running as the database/object says
# It does this through using the status_test field
# It will update the state of a component if it finds the 
# listed state is incorrect

sub validate_state {
    my $self = shift;

    unless($g) {
	$logger->error("Error, no job seems to be attached to this task, can't validate");
	die "Error, no job seems to be attached to this task, can't validate";
    }

    vertex: foreach my $v ($g->vertices) {
	my $c = $job->find_component($v);

	$logger->debug("Evaluating state for task " . $job->task_id . " component $v");

	unless($c) {
	    $logger->error("Error, we can't find component $v in job " . $job->task_id);
	    next vertex;
	}

	# Fetch the status we think the job is
	my $state = $job->find_component_state($v);

	# We only care if we think it's running
	next vertex unless($state eq 'RUNNING');
	$logger->debug("Component $v for task " . $job->task_id . " is RUNNING, validating");

	# Check the scheduler to see if the job is there
	my $sched_state = $scheduler->fetch_job_state($c->qsub_id);
	$logger->debug("Scheduler says component $v is in state $sched_state");

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
	    when ("PENDING")    { next vextex; }
	    # If it's happily running we'll leave it be
	    when ("RUNNING")    { next vertex; }
	}

	# Now we have to deal with jobs that have exited the scheduler
	# and what their current state actually is, did it succeed,
	# fail, or die silently?
	$logger->debug("Setting state for job " . $job->task_id . " to $test_state");
	given($test_state) {
	    when ("COMPLETE")   { $job->change_state({component_type => $v,
						      state => 'COMPLETE' }); }
	    when ("ERROR")      { $job->change_state({component_type => $v,
						      state => 'ERROR' }); }
	    when ("RUNNING")    { $job->change_state({component_type => $v,
						      state => 'ERROR' }); }
	}
    }

}

# For an individual component, confirm the state
# using the status_test script. Update it's status as needed

sub confirm_state {
    my $self = shift;
    my $c = shift;

    return undef unless($g->has_vertex($c));

    my $cmd = $g->get_vertex_attribute($c, 'status_test');


    $cmd =~ s/\%\%jobid\%\%/$job->job_id/e;
    $logger->debug("Running command $cmd to check state of job " . $job->task_id);

    my $rv = system($cmd);

    if($rv == -1) {
	$logger->error("Failed to execute $cmd for job " . $job->task_id);
	return "ERROR";
       
    } elsif($rv & 4) {
	$logger->debug("Task " . $job->task_id . " completed with an error");
	return "ERROR";
    } elsif($rv & 8) {
	$logger->debug("Task " . $job->task_id . " seems to still be running");
	return "RUNNING";
    }

    $logger->debug("Task " . $job->task_id . " seems to have completed successfully");
    return "COMPLETE";

}

sub run_component {
    my $self = shift;
    my $ctype = shift;

    # We can't run on a pipeline with no job attached
    return undef unless($job && $g);

    my $c = $job->fetch_component($ctype);
    unless($c) {
	$logger->error("Component $ctype not found in job " . $job->task_id . " can't start");
	return undef;
    }

    $logger->debug("Starting component $ctype for job " . $job->task_id);
    
    # Send the start request to the scheduler object
    # Name it with the task_id+component_type
    my $sched_id = $scheduler->submit_job($job->task_id . "_$ctype", $c->qsub_file);

    # Did the task submit successfully?
    if($sched_id > 0) {
	# Update the assigned scheduler task number in the component
	$job->change_state({state => "RUNNING",
			    component_type => $ctype,
			    qsub_id => $sched_id
			   });
    } else {
	# We couldn't submit the job to the scheduler, hold the job for review
	$logger->error("Error, can not submit component $ctype to scheduler for job " . $job->task_id . ", holding job");
	$job->change_state({state => "HOLD"});
    }
}

sub add_edges {
    my ($self, $origin, $vertices, $label) = @_;

    $logger->debug("Adding edges for component $origin, label $label, verticies $vertices");

    foreach my $d (split ',', $vertices) {
	$g->add_edge($origin, $d);
	$g->set_edge_attribute($origin, $d, $label, 1);
    }
}

sub find_entry_points {
    my $self = shift;

    my @v = $g->source_vertices();

    print Dumper @v;
    return @v;
}

sub attach_job {
    my $self = shift;
    $job = shift;

    $self->overlay_job;

    my $sched = $job->job_scheduler;
    # will this work?
    # $scheduler = "MetaScheduler::$job->job_scheduler"->instance();
    eval {
	no strict 'refs';
	$scheduler = "MetaScheduler::$sched"->instance();
    }
}

# We call this each time the scheduler wants to give the
# job a turn to run a step

sub run_iteration {
    my $self = shift;

    # We can't run an iteration if there's no job attached
    unless($job && $g) {
	$logger->error("Error, can't run an iteration on pipeline, nob job attached");
	die "Error, can't run an iteration on pipeline, nob job attached";
    }

    $logger->debug("Running iteration of job " . $job->job_type . " with job_id " . $job->job_id);

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

    # Now go through again and find the components that are pending
    # and all their parents are complete, following the success/failure
    # path
    $logger->debug("Walking the job looking for components to run");
    foreach my $start (@starts) {
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

    $logger->debug("Walking component $v");

    my $state = $job->find_component_state($v);

    given($job->find_component_state($v)) {
	when ("PENDING")    { 
	    # Unless all the predecessors are complete or error
	    # we can't run this component
	    $logger->debug("Component $v is pending, checking parents");
#	    foreach my $u ($g->predecessors($v)) {
#		my $s = $job->find_component_state($u);
#		return unless(($s eq 'COMPLETE') || ($s eq 'ERROR'));
#	    }

	    return unless($self->is_runable($v));

	    $logger->debug("Looks good for $v, trying to run");
	    $self->run_component($v);
	    return;
	}
	when ("COMPLETE")   { 
	    $logger->debug("Component $v complete, walking children");
	    foreach my $u ($g->successors($v)) {
		$self->walk_and_run($v);
	    }
	}
	when ("HOLD")       { return; }
	when ("ERROR")      { 
	    $logger->debug("Component $v error, walking children");
	    foreach my $u ($g->successors($v)) {
		$self->walk_and_run($v);
	    }
	}
	when ("RUNNING")    { return; }
    }
    
}

sub is_runable {
    my $self = shift;
    my $v = shift;

    return 0 unless($job->find_component_state($v) eq "PENDING");

    foreach my $u ($g->predecessors($v)) {
	my $s = $job->find_component_state($u);
	return 0 unless(($s eq 'COMPLETE') || ($s eq 'ERROR'));
    }

    return 1;
}

# Fetch the job component of the task

sub fetch_job {
    my $self = shift;

    return $job if($job);

    return undef;
}

# Shortcut to fetch the task_id

sub fetch_task_id {
    my $self = shift;

    return $job->task_id if($job);

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

    my $states = $job->find_all_state;

    if($states->{"RUNNING"}) {
	$job->run_status("RUNNING");
	return;
    } elsif($states->{"HOLD"}) {
	$job->run_status("HOLD");
	return;
    }

    my @starts = $self->find_entry_points();
    my $started = 0;
    foreach my $v (@starts) {
	$started = 1 unless($job->find_component_state($v) eq "PENDING");
    }
    unless($started) {
	$job->run_status("PENDING");
	return;
    }

    my $ended = 1;
    my $error = 0;
    foreach my $v ($g->sink_verticies()) {
	my $s = $job->find_component_state($v);
	$ended = 0 unless($s eq 'COMPLETE' ||
			  $s eq 'ERROR');
	$error = 1 if($s eq 'ERROR');
    }
    if($ended) {
	if($error) {
	    $job->run_status('ERROR');
	}
	$job->run_status('COMPLETE');
	return;
    }

    # Now we're in to the weird states, either stuck or
    # just nothing happens to be in the queue
    $logger->error("We seem to be stuck in this job, that's not good. Job:" . $job->task_id);

}

# Return an array of all runable components
# Runable means all their parents are complete or
# error (or it's a start point)

sub find_runable {
    my $self = shift;
    my $v = shift;

    return unless($job && $g);

    my $runable;
    $logger->debug("Finding runable components for pipeline");

    unless($v) {
	my @starts = $self->find_entry_points;

	foreach my $start (@starts) {
	    $logger->debug("Checking start point $start");
	    if($self->is_runable($start)) {
		push @{$runable}, $start;
		$logger->debug("Start point $start is runable");
		next;
	    }

	    foreach my $u ($g->successors($start)) {
		push @{$runable}, $self->find_runable($u);
	    }

	}

#	@{$runable} = grep { ! $seen{ $_ }++ } @{$runable};
	@{$runable} = uniq(@{$runable});
	return $runable;
    }

    $logger->debug("Checking state for $v");
    my $state = $job->find_component_state($v);

    given($job->find_component_state($v)) {
	when ("PENDING")    { 
	    # Unless all the predecessors are complete or error
	    # we can't run this component
	    $logger->debug("Component $v is pending, checking parents");
#	    foreach my $u ($g->predecessors($v)) {
#		my $s = $job->find_component_state($u);
#		return unless(($s eq 'COMPLETE') || ($s eq 'ERROR'));
#	    }

	    return unless($self->is_runable($v));

	    $logger->debug("Looks good for $v, is runable");
	    return $v;
	}
	when ("COMPLETE")   { 
	    $logger->debug("Component $v complete, walking children");
	    foreach my $u ($g->successors($v)) {
		push @{$runable}, $self->find_runable($u);
	    }
	    ($runable ? return @{$runable} : return );
	}
	when ("HOLD")       { return; }
	when ("ERROR")      { 
	    $logger->debug("Component $v error, walking children");
	    foreach my $u ($g->successors($v)) {
		push @{$runable}, $self->find_runable($u);
	    }
	    ($runable ? return @{$runable} : return );
	}
	when ("RUNNING")    { return; }
    }
 
}

sub overlay_job {
    my $self = shift;

    unless(($job->run_status eq "RUNNING") ||
	   ($job->run_status eq "ERROR") ||
	   ($job->run_status eq "COMPLETE")) {

	$logger->info("Job is in state " . $job->run_state . ", not overlaying over pipeline");
	return;
    }

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

    $logger->debug("Walking component $v");

    # Is is a sink vertex, or a vertex with no children
    if($g->is_sink_vertex($v)) {
	$logger->debug("Vertex $v is a sink, stopping");
	return;
    } else {
	my $state = $job->find_component_state($v);

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
	    $logger->debug("Component $v is PENDING, no where to go");
	    return;

	} elsif($state eq "HOLD") {
	    # Component is on hold, we stop here because we
	    # don't know which direction to go
	    $logger->debug("Component $v is HOLD, no where to go");
	    return;

	} elsif($state eq "RUNNING") {
	    # Component is running, we stop here because we
	    # don't know which direction to go
	    $logger->debug("Component $v is RUNNING, no where to go");
	    return;

	} else {
	    $logger->error("Unknown state for component $v");
	    die "Error, unknown state for component $v";
	}

	# Recursively walk the remaining edges, if any
	foreach my $u ($g->successors($v)) {
	    $self->overlay_walk_component($u);
	}
    }
}

sub remove_edges {
    my $self = shift;
    my $v = shift;
    my $attr = shift;

    foreach my $u ($g->successors($v)) {

	if($g->has_edge_attribute($v, $u, $attr)) {
	    $logger->debug("Removing attribute from edge $v, $u: $attr");
	    $g->delete_edge_attribute($v, $u, $attr);
	    unless($g->has_edge_attributes($v, $u)) {
		$logger->debug("Removing edge $v, $u with value $attr");
		$g->delete_edge($v, $u);
		$self->scrub_dangling_vertices($u);
	    } else {
		$logger->debug("Multiple attributes on edge $v, $u, not deleting");
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

    return unless($g->is_predecessorless_vertex($v));

    my @successors = $g->successors($v);

    $logger->debug("Vertex $v has no parents, removing");
    $g->delete_vertex($v);

    foreach my $u (@successors) {
	$self->scrub_dangling_vertices($u);
    }
}

sub fetch_component {
    my ($self, $name) = @_;

    die "Error, no pipeline loaded"
	unless($pipeline);

    $logger->debug("Returning pipeline component $name");

    foreach my $component (@{$pipeline->{'components'}}) {
	return $component
	    if($name eq $component->{'name'});
    }

    return 0;
}

sub graph {
    my $self = shift;
    my $fields = shift;

    # We can do a graph unless we have a job attached
    return unless($job);

    my $gv = GraphViz2->new(global => {directed => 1});

    foreach my $v ($g->vertices) {
	$self->graph_node($gv, $v, $fields);
#	$gv->add_node(name => $v);
    }

    foreach my $e ($g->edges) {
	if($g->has_edge_attribute($e->[0], $e->[1], 'success') && $g->has_edge_attribute($e->[0], $e->[1], 'failure')) {
	    $gv->add_edge(from => $e->[0], to => $e->[1]);
	} elsif($g->has_edge_attribute($e->[0], $e->[1], 'success')) {
	    $gv->add_edge(from => $e->[0], to => $e->[1], color => 'green', label => 'on success');
	} elsif($g->has_edge_attribute($e->[0], $e->[1], 'failure')) {
	    $gv->add_edge(from => $e->[0], to => $e->[1], color => 'red', label => 'on failure');
	}
    }

    $gv->run(format => 'svg', output_file => $cfg->{jobs_dir} . '/' . $job->task_id . '/' . "graph.svg");
}

sub graph_node {
    my $self = shift;
    my $gv = shift;
    my $v = shift;
    my $fields = shift;

    my $c;

    return unless($c = $job->find_component($v));

    print Dumper MetaScheduler::Component->meta->get_attribute_list;

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

sub dump_graph {
    print "Graph $g\n";
}

1;
