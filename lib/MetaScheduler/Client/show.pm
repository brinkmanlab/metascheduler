=head1 NAME

    MetaScheduler::Client::show

=head1 DESCRIPTION

    Object for handling query requests to the scheduler
    Should be called from the client script and gets it's
    configuration from the @ARGV parameters

=head1 SYNOPSIS

    use MetaScheduler::Client::show;
    MetaScheduler::Client::submit->initialize($host, $port, @ARGV);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    September 5, 2013

=cut

package MetaScheduler::Client::show;

use strict;
use warnings;
use JSON;
use MetaScheduler::Client;

use Getopt::Long;

my $protocol_version = '1.0';

sub initialize {
    my ($self, $host, $port, @ARGV) = @_;

    my $type; my $user; my $passwd; my $raw;
    my $help;

    my $res = GetOptions ("type=s"  => \$type,
			  "raw"     => \$raw,
			  "user=s"   => \$user,
			  "passwd=s" => \$passwd,
			  "help"    => \$help,
	)
	or die "Error in command line arguments";

    if($help) {
	usage();
    }

    die "Error, no user or password"
	unless($user && $passwd);

    my $client = MetaScheduler::Client->new();
    $client->connect($host, $port);

    my $str = buildReq($user, $passwd, $type);

    my $ret = $client->send_req($str);

    if($raw) {
	print $ret;
    } else {
	printResults($ret);
    }
}

sub usage {
    print "Usage: $0 show [job_type] [--raw|-r]\n";
    exit; 
}

sub buildReq {
    my $user = shift;
    my $passwd = shift;
    my $type = shift;

    my $str = "{\n \"version\": \"$protocol_version\",\n";
    $str .= " \"userid\": \"$user\",\n";
    $str .= " \"password\": \"$passwd\",\n";
    $str .= " \"action\": \"show\"\n";
    $str .= ", \"type\": \"$type\"\n"
	if($type);
    $str .= "}\nEOF\n";

    return $str;
}

# Interpret the results and make them pretty

sub printResults {
    my $json = shift;

    my $res;
    eval {
	$res = decode_json($json);
    };
    if($@) {
	die "Error, we can't interpret the results received from the server";
    }

    # If we received a 500 something went wrong with the request
    if($res->{code} eq '500') {

	print "Error, something went wrong with the request to the scheduler\n";
	return;

    # If we received a 404, that means there were no records found
    } elsif($res->{code} eq '404') {
	print "No jobs found\n";
	return;
    }

    # Print the reader
    print "Task Id\tName\tType\tStatus\n";

    for my $job (@{$res->{results}}) {
	print $job->{task_id} . "\t" . $job->{job_name} ."\t" . $job->{job_type} . "\t" . $job->{run_status} . "\n";
    }
}

1;
