=head1 NAME

    Job.pm

=head1 DESCRIPTION

    Object for holding and managing an individual job

=head1 SYNOPSIS

    use Job;

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

package Job;

use strict;
use warnings;
use JSON;
use Data::Dumper;
use Moose;
use Carp qw( confess );
use DBISingleton;

# Job object
my $job;

sub BUILD {
    my $self = shift;
    my $args = shift;
    
    return unless($args->{job});

    eval {
	$job = decode_json($args->{job});
    };
    if($@) {
	# Error evaluating job's json
	die "Error evaluating job: $args->{job}, $@";
    }

    my $dbh = DBISingleton->dbh;

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

sub dump {
    return unless($job);

    print Dumper $job;
}

1;
