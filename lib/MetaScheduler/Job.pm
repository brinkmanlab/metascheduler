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
use Log::Log4perl;
use JSON;
use Data::Dumper;
use Moose;
use Moose::Util::TypeConstraints;
use Carp qw( confess );
use MetaScheduler::DBISingleton;
use MetaScheduler::Config;
use MetaScheduler::Component;
use MetaScheduler::Mailer;

# Job object
# Do we need to keep this if it's only a parsing step?
#my $job;

has task_id => (
    is     => 'rw',
    isa    => 'Int'
);

has run_status => (
    is     => 'rw',
    isa    => enum([qw(PENDING COMPLETE HOLD ERROR RUNNING)])
);

has job_id => (
    is     => 'rw',
    isa    => 'Int'
);

has job_type => (
    is     => 'rw',
    isa    => 'Str'
);

has job_name => (
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

has job_scheduler => (
    is      => 'rw',
    isa     => 'Str',
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

has mailer => (
    is      => 'rw',
    isa     => 'Ref',
);

my $logger; my $cfg;

sub BUILD {
    my $self = shift;
    my $args = shift;
    
    $logger = Log::Log4perl->get_logger;

    $cfg =  MetaScheduler::Config->config;

    if($args->{job}) {
    # First case, we're given a JSON job definition to load
    # in to the database
	eval {
	    $self->{job} = decode_json($args->{job});
	};
	if($@) {
	    # Error evaluating job's json
	    die "Error evaluating job: $args->{job}, $@";
	}

	# Validate the submission and load the job in to the database

	$self->task_id($self->create_job($self->{job}));
	$self->load_job($self->task_id);
	$self->load_components($self->task_id);
	$self->load_mailer($self->task_id);
	$self->makeWorkdir();

	return;

    } elsif($args->{task_id}) {
    # We're pulling a job from the database based on it's
    # internal task_id

	$logger->debug("Loading task $args->{task_id}");
	$self->load_job($args->{task_id});
	$self->load_components($args->{task_id});
	$self->load_mailer($args->{task_id});
	$self->makeWorkdir();

	return;

    } elsif($args->{job_id} && $args->{job_type}) {
    # We're pulling a job from the database based on it's
    # external job ID and type

	my $dbh = MetaScheduler::DBISingleton->dbh;

	my $sqlstmt = qq{SELECT task_id FROM task WHERE job_id = ? AND job_type = ?};
	my $find_task_id = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";
	$find_task_id->execute($args->{job_id}, $args->{job_type}) or
	    die "Error fetching task_id ($args->{job_id}, $args->{job_type}): $DBI::errstr";

	if(my @row = $find_task_id->fetchrow_hashref) {
	    $self->load_job($row[0]);
	    $self->load_components($row[0]);
	    $self->load_mailer($row[0]);
	    $self->makeWorkdir();

	} else {
	    $logger->error("Error, can not find task_id for $args->{job_id}, $args->{job_type}");
	    die "Error, can not find task_id for $args->{job_id}, $args->{job_type}";
	}

	return;
       }

    die "Error, you can't have an empty job object";

}

sub find_jobs {
    my $self = shift;
    my $job_type = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT task_id, job_id, job_name, run_status FROM task WHERE job_type = ?};
    my $fetch_jobs =  $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

#    $logger->debug("Fetching jobs for job_type $job_type");
    $fetch_jobs->execute($job_type) or
	die "Error fetching jobs for job_type $job_type: $DBI::errstr";

    my @jobs;
    while(my @row = $fetch_jobs->fetchrow_array) {
	push @jobs, [@row];
    }

    return \@jobs;
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
	$self->{job} = decode_json($json);
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

    $logger->debug("Creating and saving job $args->{job_type} for $args->{job_id}, $args->{job_name}");

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{INSERT INTO task (job_id, job_type, job_name, job_scheduler, extra_parameters, priority) VALUES (?, ?, ?, ?, ?)};
    my $add_job = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $add_job->execute($args->{job_id}, $args->{job_type}, $args->{job_name}, $args->{job_scheduler} || $cfg->{default_scheduler}, $args->{extra_parameters} || '', $args->{priority} || 2) or
	die "Error inserting job ($args->{job_id}, $args->{job_type}, $args->{job_name})";

    my $task_id = $dbh->last_insert_id( undef, undef, undef, undef );
    $self->task_id($task_id);

    die "Error, no task_id returned ($args->{job_id}, $args->{job_type}, $args->{job_name})"
	unless($task_id);

    my $c_obj;
    for my $component (@{$args->{components}}) {
	eval {

	    $c_obj = MetaScheduler::Component->new({ %$component,
							task_id => $task_id
						      });
	};
	if($@) {
	    $logger->error("We weren't able to make the component $component->{component_type} for task $task_id ($args->{job_type}, $args->{job_name}): $@");
	    $self->change_state({state => 'ERROR',
				 task_id => $task_id
				});
	    die "Error, can not create component for task $task_id";
	}
	push @{ $self->components }, $c_obj;
    }

    # Create mailer object if it exists
    if($args->{job_email}) {
	my @emails;
	foreach my $email_obj (@{$args->{job_email}}) {
	    print Dumper $email_obj;
	    if($email_obj->{email}) {
	    push @emails, $email_obj->{email};
	    }
	}

	print Dumper @emails;
	$self->add_emails(@emails);
    }

    return $task_id;
}

sub change_state {
    my $self = shift;
    my $args = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    if($args->{component_type}) {

	my $component = $self->find_component($args->{component_type});
	
	if($component) {
	    $logger->debug("Changing state for component, punting on to component object");
	    $component->change_state($args);
	} else {
	    $logger->error("Component $args->{component_type} not found");
	}

    } else {
	my $task_id;
	$logger->debug("Changing state for job " . $self->task_id . " to $args->{state}");

	if(uc($args->{state}) eq 'COMPLETE') {
	    $dbh->do("UPDATE task SET run_status = \"COMPLETE\", complete_date= NOW() WHERE task_id = ?", {}, $self->task_id);
	} elsif(uc($args->{state}) eq 'HOLD') {
	    $dbh->do("UPDATE task SET run_status = \"HOLD\" WHERE task_id = ?", {}, $self->task_id);
	} elsif(uc($args->{state}) eq 'ERROR') {
	    $dbh->do("UPDATE task SET run_status = \"ERROR\", complete_date= NOW() WHERE task_id = ?", {}, $self->task_id);
	} elsif(uc($args->{state}) eq 'RUNNING') {
	    $dbh->do("UPDATE task SET run_status = \"RUNNING\", start_date= NOW() WHERE task_id = ?", {}, $self->task_id);
	} else {
	    $logger->error("State requested for job " . $self->task_id . " of $args->{state} doesn't exist!");
	    die "State requested for job " . $self->task_id . " of $args->{state} doesn't exist";
	}

	# Reload the job
	$self->load_job($self->task_id);

    }


}

sub find_component_state {
    my $self = shift;
    my $component_type = shift;

    my $c = $self->find_component($component_type);

    if($c) {
	return $c->run_status;
    } else {
	$logger->("Error, can't find component $component_type when checking state");
	return undef;
    }
}

sub find_component {
    my $self = shift;
    my $component_type = shift;

    foreach my $c (@{$self->components}) {
	if(lc($c->component_type) eq lc($component_type)) {
	    return $c;
	}
    }

    $logger->debug("Component $component_type not found");
    return undef;
}

# Return a count of all the state types

sub find_all_state {
    my $self = shift;

    my $state_count;

    foreach my $c (@{$self->components}) {
	$state_count->{$c->run_status} += 1;
    }

    return $state_count;
}

sub load_job {
    my $self = shift;
    my $task_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT run_status, job_id, job_type, job_name, job_scheduler, extra_parameters, priority, UNIX_TIMESTAMP(submitted_date) AS submitted_date, UNIX_TIMESTAMP(start_date) AS start_date, UNIX_TIMESTAMP(complete_date) AS complete_date FROM task WHERE task_id = ?};
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

	push @{ $self->components }, $c;
    }
}

sub load_mailer {
    my $self = shift;
    my $task_id = shift;

    $logger->debug("Loading mailer object for task $task_id");
    my $mailer = MetaScheduler::Mailer->new({task_id => $task_id});

    $self->mailer($mailer);
}

sub add_emails {
    my $self = shift;
    my @emails = @_;

    return unless(@emails);

    print Dumper @emails;

    if($self->mailer) {
	$logger->debug("Adding to existing mailer object task_id " . $self->task_id . ", emails @emails");
	$self->mailer->add_email($self->task_id, @emails);
    } else {
	$logger->debug("Creating new mailer object for task_id " .$self->task_id);
	my $mailer = MetaScheduler::Mailer->new({task_id => $self->task_id, emails => \@emails});
	$self->mailer($mailer);
    }
}

sub makeWorkdir {
    my $self = shift;
    
    unless( -d  $cfg->{jobs_dir} . '/' . $self->task_id ) {
	mkdir $cfg->{jobs_dir} . '/' . $self->task_id
	    or die "Error making workdir for " . $self->task_id . " : $@";
    }
}

1;
