=head1 NAME

    Brinkman::MetaScheduler::Job

=head1 DESCRIPTION

    Object for holding and managing an individual job

=head1 SYNOPSIS

    use Brinkman::MetaScheduler::Job;

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

package Brinkman::MetaScheduler::Job;

use strict;
use warnings;
use JSON;
use Data::Dumper;
use Moose;
use Moose::Util::TypeConstraints;
use Carp qw( confess );
use Brinkman::MetaScheduler::DBISingleton;

# Job object
# Do we need to keep this if it's only a parsing step?
my $job;

has task_id => (
    is     => 'ro',
    isa    => 'Int'
);

has run_status => (
    is     => 'ro',
    isa    => enum([qw(PENDING COMPLETE HOLD ERROR RUNNING)]) );
);

has job_id => (
    is     => 'ro',
    isa    => 'Int'
);

has job_type => (
    is     => 'ro',
    isa    => 'Str'
);

has extra_parameters => (
    is     => 'ro',
    isa    => 'Str'
);

has priority => (
    is     => 'ro',
    isa    => 'Int'
);

# Use epoch, which means converting
# from a mysql timestamp when reading/writing
# from the database
# idea: http://search.cpan.org/~doy/Moose-2.0604/lib/Moose/Manual/FAQ.pod#How_can_I_inflate/deflate_values_in_accessors?
has submitted_date  => (
    is     => 'ro',
    isa    => 'Int'
);

# But how to represent a non started job in epoch?
# ignore the problem and depend on run_status?
has start_date  => (
    is     => 'ro',
    isa    => 'Int'
);

has complete_date  => (
    is     => 'ro',
    isa    => 'Int'
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

	return;

    } elsif($args->{task_id}) {
    # We're pulling a job from the database based on it's
    # internal task_id

	return;

    } elsif($args->{job_id} && $args->{job_type}) {
    # We're pulling a job from the database based on it's
    # external job ID and type

	return;
    }

    die "Error, you can't have an empty job object";

    my $dbh = Brinkman::MetaScheduler::DBISingleton->dbh;

    $dbh->do("SELECT SLEEP(10)");

#    $self->has_submission and eval {
#	$job = decode_json($self->submission);
#    };
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

sub _load_job_to_db {
    my $self = shift;
    
    my $dbh = Brinkman::MetaScheduler::DBISingleton->dbh;

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
