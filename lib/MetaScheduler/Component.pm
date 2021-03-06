=head1 NAME

    MetaScheduler::Component

=head1 DESCRIPTION

    Object for holding components of an individual job

    Not invoked on it's own, an internal object of Job

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    March 27, 2013

=cut

package MetaScheduler::Component;

use strict;
use warnings;
use Log::Log4perl;
use Moose;
use Moose::Util::TypeConstraints;
use MetaScheduler::DBISingleton;
use Carp qw( confess );

has component_id => (
    is     => 'rw',
    isa    => 'Int'
);

has task_id => (
    is     => 'rw',
    isa    => 'Int'
);

has component_type => (
    is     => 'rw',
    isa    => 'Str'
);

has run_status => (
    is     => 'rw',
    isa    => enum([qw(PENDING COMPLETE HOLD ERROR RUNNING)] )
);

has extra_parameters => (
    is      => 'rw',
    isa     => 'Str',
    default => ''
);

has qsub_file => (
    is     => 'rw',
    isa    => 'Str'
);

has qsub_id => (
    is     => 'rw',
    isa    => 'Str'
);

has start_date  => (
    is     => 'rw',
    isa    => 'Int'
);

has complete_date  => (
    is     => 'rw',
    isa    => 'Int'
);

my $logger;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger unless($logger);

    if($args->{component_id}) {
	# Load an existing component from the database
	$self->load_component($args->{component_id});
    } else {
	# Make a new component and add it to the database

	$self->load_component($self->create_component($args));
    }
}

sub find_components {
    my $self = shift;
    my $task_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT component_id FROM component WHERE task_id = ?};
    my $fetch_components =  $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

#    $logger->debug("Fetching components for task_id $task_id");
    $fetch_components->execute($task_id);

    my @components;
    while(my @row = $fetch_components->fetchrow_array) {
	push @components, $row[0];
    }

    return @components;

}

sub change_state {
    my $self = shift;
    my $args = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    $logger->trace("Changing state for component " . $self->component_id . " to $args->{state}, [" . $self->component_type . '], [' . $self->task_id . ']');

    if(uc($args->{state}) eq 'COMPLETE') {
	$dbh->do("UPDATE component SET run_status = \"COMPLETE\", complete_date= NOW() WHERE component_id = ?", {}, $self->component_id);
    } elsif(uc($args->{state}) eq 'HOLD') {
	$dbh->do("UPDATE component SET run_status = \"HOLD\" WHERE component_id = ?", {}, $self->component_id);
    } elsif(uc($args->{state}) eq 'PENDING') {
	$dbh->do("UPDATE component SET run_status = \"PENDING\" WHERE component_id = ?", {}, $self->component_id);
    } elsif(uc($args->{state}) eq 'ERROR') {
	$dbh->do("UPDATE component SET run_status = \"ERROR\", complete_date= NOW() WHERE component_id = ?", {}, $self->component_id);
    } elsif(uc($args->{state}) eq 'RUNNING') {
	$dbh->do("UPDATE component SET run_status = \"RUNNING\", start_date= NOW(), qsub_id = ? WHERE component_id = ?", {}, $args->{qsub_id}, $self->component_id);
    } else {
	$logger->error("State requested for component " . $self->component_id . " of $args->{state} doesn't exist, [" . $self->component_type . '], [' . $self->task_id . ']');
	die "State requested for component " . $self->component_id . " of $args->{state} doesn't exist, [" . $self->component_type . '], [' . $self->task_id . ']';
    }

    # Reload the component
    $self->load_component($self->component_id);
}

sub set_sched_id {
    my $self = shift;
    my $sched_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    $logger->debug("Changing sched_id (qsub_id) for component " . $self->component_id . " to $sched_id, [" . $self->component_type . '], [' . $self->task_id . ']');

    $dbh->do("UPDATE component SET qsub_id = ? WHERE component_id = ?", {}, $sched_id, $self->component_id);

    # Reload the component
    $self->load_component($self->component_id);
}

sub create_component {
    my $self = shift;
    my $args = shift;

    $logger->debug("Creating and saving component $args->{component_type} for task_id [$args->{task_id}]");

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{INSERT INTO component (task_id, component_type, extra_parameters, qsub_file) VALUES (?, ?, ?, ?)};
    my $add_component = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $add_component->execute($args->{task_id}, $args->{component_type}, $args->{extra_parameters} || '', $args->{qsub_file} || '') or
	die "Error inserting component ([$args->{task_id}], $args->{component_type})";
    
    my $component_id = $dbh->last_insert_id( undef, undef, undef, undef );

    die "Error, no component_id returned ([$args->{task_id}], [$args->{component_type}])"
	unless($component_id);

    return $component_id;
}

sub load_component {
    my $self = shift;
    my $component_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT task_id, component_type, run_status, extra_parameters, qsub_file, qsub_id, UNIX_TIMESTAMP(start_date) as start_date, UNIX_TIMESTAMP(complete_date) as complete_date FROM component WHERE component_id = ?};
    my $fetch_component =  $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $logger->debug("Fetching component $component_id");
    $fetch_component->execute($component_id);

    if(my $row = $fetch_component->fetchrow_hashref) {
	# Load the pieces
	for my $k (keys %$row) {
	    $self->$k($row->{$k});
	}

	$self->component_id($component_id)
    } else {
	$logger->error("Can't find component $component_id");
	die("Can't find component $component_id");
    }
}

sub dump_json {
    my $self = shift;
    my $indent = shift;

    my $json_data = $self->component_struct();

    my $json = to_json($json_data, {pretty => 1});

    return $json;
}

sub component_struct {
    my $self = shift;

    my $json_data;

    $json_data->{component_id} =  $self->component_id;
    $json_data->{component_type} = $self->component_type;
    $json_data->{run_status} = $self->run_status;
    $json_data->{extra_parameters} = $self->extra_parameters;
    $json_data->{qsub_file} = $self->qsub_file;
    $json_data->{qsub_id} = $self->qsub_id;
    $json_data->{start_date} = $self->start_date;
    $json_data->{complete_date} = $self->complete_date;

    return $json_data;

#    my $json = "$indent" . "{\n";
#    $json .= "$indent \"component_id\": \"" . $self->component_id . "\",\n";
#    $json .= "$indent \"component_type\": \"" . $self->component_type . "\",\n";
#    $json .= "$indent \"run_status\": \"" . $self->run_status . "\",\n";
#    $json .= "$indent \"extra_parameters\": \"" . $self->extra_parameters . "\",\n";
#    $json .= "$indent \"qsub_file\": \"" . $self->qsub_file . "\",\n";
#    $json .= "$indent \"qsub_id\": \"" . $self->qsub_id . "\",\n";
#    $json .= "$indent \"start_date\": \"" . $self->start_date . "\",\n";
#    $json .= "$indent \"complete_date\": \"" . $self->complete_date . "\"\n";
#    $json .= "$indent}";

#    return $json;
}

1;
