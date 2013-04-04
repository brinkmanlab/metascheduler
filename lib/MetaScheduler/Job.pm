=head1 NAME

    MetaScheduler::Job

=head1 DESCRIPTION

    Object for holding and managing an individual job

=head1 SYNOPSIS

    use MetaScheduler::Job;

    # Submitted jobs should always be evaluated in an eval
    # block to catch any errors parsing the JSON
    my $job;
    eval {
      $job = Job->new(job => $JSONstr);
    };

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    March 27, 2013

=cut

package MetaScheduler::Job;

use strict;
use warnings;
use JSON;
use Data::Dumper;
use Moose;
use Moose::Util::TypeConstraints;
use Carp qw( confess );
use MetaScheduler::DBISingleton;
use MetaScheduler::Component;

# Job object
# Do we need to keep this if it's only a parsing step?
my $job;

has task_id => (
    is     => 'rw',
    isa    => 'Int'
);

has run_status => (
    is     => 'rw',
    isa    => enum([qw(PENDING COMPLETE HOLD ERROR RUNNING)]) );
);

has job_id => (
    is     => 'rw',
    isa    => 'Int'
);

has job_type => (
    is     => 'rw',
    isa    => 'Str'
);

has extra_parameters => (
    is     => 'rw',
    isa    => 'Str'
);

has priority => (
    is     => 'rw',
    isa    => 'Int'
);

# Use epoch, which means converting
# from a mysql timestamp when reading/writing
# from the database
# idea: http://search.cpan.org/~doy/Moose-2.0604/lib/Moose/Manual/FAQ.pod#How_can_I_inflate/deflate_values_in_accessors?
has submitted_date  => (
    is     => 'rw',
    isa    => 'Int'
);

# But how to represent a non started job in epoch?
# ignore the problem and depend on run_status?
has start_date  => (
    is     => 'rw',
    isa    => 'Int'
);

has complete_date  => (
    is     => 'rw',
    isa    => 'Int'
);

has components => (
    traits  => ['Array'],
    is      => 'rw',
    isa     => 'ArrayRef[Ref]',
    default => sub { [] },
);

sub BUILD {
    my $self = shift;
    my $args = shift;
    
    return unless($args->{job});
    if(($args->{job}) {
    # First case, we're given a JSON job definition to load
    # in to the database
	eval {
	    $job = decode_json($args->{job});
	};
	if($@) {
	    # Error evaluating job's json
	    die "Error evaluating job: $args->{job}, $@";
	}

	# Validate the submission and load the job in to the database

	$self->load_job($self->create_job($job));

	return;

    } elsif($args->{task_id}) {
    # We're pulling a job from the database based on it's
    # internal task_id

	$self->load_job($args->{task_id});
	$self->load_components($args->{task_id});
	return;

    } elsif($args->{job_id} && $args->{job_type}) {
    # We're pulling a job from the database based on it's
    # external job ID and type

	my $dbh = MetaScheduler::DBISingleton->dbh;

	my $sqlstmt = qq{SELECT task_id FROM task WHERE job_id = ? AND job_type = ?};
	my $find_task_id = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";
	$find_task_id->execute($args->{job_id}, $args->{job_type}) or
	    die "Error fetching task_id ($args->{job_id}, $args->{job_type}): $DBI::errstr";

	if(my $row = $find_task_id->fetchrow_hashref) {
	    $self->load_job($row[0]);
	    $self->load_components($row[0]);
	} else {
	    $logger->error("Error, can not find task_id for $args->{job_id}, $args->{job_type}");
	    die "Error, can not find task_id for $args->{job_id}, $args->{job_type}";
	}

	return;
       }

    die "Error, you can't have an empty job object";

}

sub read_job {
    my ($self, $job_file) = @_;

#    print "Reading job file $job_file\n";

    return -1
	unless ( -f $job_file && -r $job_file );

    my $json;
    {
	local $/; #enable slurp
	open my $fh, "<", "$job_file";
	$json = <$fh>;
    } 

    eval {
	$job = decode_json($json);
    };
    if($@) {
	# Error decoding JSON
	return 0;
    }

    return 1;
}

sub create_job {
    my $self = shift;
    my $args = shift;

    $logger->debug("Creating and saving job $args->{job_type} for $args->{job_id} $args->{job_name}");

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{INSERT INTO task (job_id, job_type, job_name, extra_parameters, priority) VALUES (?, ?, ?, ?, ?)};
    my $add_job = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $add_job->execute($args->{job_id}, $args->{job_type}, $args->{job_name}, $extra_parameters || '', $args->{priority} || "DEFAULT") or
	die "Error inserting job ($args->{job_id}, $args->{job_type}, $args->{job_name})";

    my $task_id = $dbh->last_insert_id( undef, undef, undef, undef );

    die "Error, no task_id returned ($args->{job_id}, $args->{job_type}, $args->{job_name})"
	unless($task_id);

    for my $component (@{$args->{components}}) {
	eval {

	    my $c_obj = MetaScheduler::Component->new({ %$component,
							task_id => $task_id
						      });
	};
	if($@) {
	    $logger->error("We weren't able to make the component $component->{component_type} for task $task_id ($args->{task_type}, $args->{task_name}");
	    $self->change_state({state => 'FAILED',
				 task_id => $task_id
				});
	    die "Error, can not create component for task $task_id";
	}
	push @{ $self->components }, $c_obj;
    }

    return $task_id;
}

sub change_state {
    my $self = shift;
    my $args = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    $logger->debug("Changing state for job $args->{task_id} to $args->{state}");

    if(uc($args->{state}) eq 'COMPLETE') {
	$dbh->do("UPDATE task SET run_status = \"COMPLETE\", complete_date= NOW() WHERE task_id = $args->{task_id}");
    } elsif(uc($args->{state}) eq 'HOLD') {
	$dbh->do("UPDATE task SET run_status = \"HOLD\" WHERE task_id = $args->{task_id}");
    } elsif(uc($args->{state}) eq 'ERROR') {
	$dbh->do("UPDATE task SET run_status = \"ERROR\", complete_date= NOW() WHERE task_id = $args->{task_id}");
    } elsif(uc($args->{state}) eq 'RUNNING') {
	$dbh->do("UPDATE task SET run_status = \"RUNNING\", start_date= NOW() WHERE task_id = $args->{task_id}");
    }

    # Reload the job
    $self->load_job($args->{task_id});
}


sub load_job {
    my $self = shift;
    my $task_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT run_status, job_id, job_type, job_name, extra_paramters, priority, UNIX_TIMESTAMP(submitted_date) AS submitted_date, UNIX_TMESTAMP(start_date) AS start_date, UNIX_TIMESTAMP(complete_date) AS complete_date FROM task WHERE task_id = ?};
    my $fetch_job = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $logger->debug("Fetching job $task_id");
    $fetch_job->execute($task_id);

    if(my $row = $fetch_job->fetchrow_hashref) {
	# Load the pieces
	for my $k (keys %$row) {
	    $self->$k($row->{$k});
	}

	$self->task_id($task_id)
    } else {
	$logger->error("Can't find job $task_id");
	die("Can't find job $task_id");
    }

}

sub load_components {
    my $self = shift;
    my $task_id = shift;

    my @components = MetaScheduler::Component->find_components($task_id);

    foreach my $cid (@components) {
	my $c = MetaScheduler::Component->new({component_id => $cid });

	push @{ $self->components }, $c_obj;
    }
}

sub _load_job_to_db {
    my $self = shift;
    
    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{INSERT INTO task (job_id, job_type, job_name, extra_parameters, priority) VALUES (?, ?, ?, ?, ?)};
    my $insert_task = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $sqlstmt = qq{INSERT INTO component (task_id, component_type, extra_parameters, qsub_file) VALUES (?, ?, ?, ?)};
    my $insert_component = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

}

sub dump {
    return unless($job);

    print Dumper $job;
}

1;