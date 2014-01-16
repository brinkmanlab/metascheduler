=head1 NAME

    MetaScheduler::Client::logging

=head1 DESCRIPTION

    Alter the logging level on the scheduler

=head1 SYNOPSIS

    use MetaScheduler::Client::logging;
    MetaScheduler::Client::logging->initialize($host, $port, @ARGV);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    January 16, 2014

=cut

package MetaScheduler::Client::logging;

use strict;
use warnings;
use JSON;
use MetaScheduler::Client;

use Getopt::Long;

my $protocol_version = '1.0';

my @logging_levels = qw/TRACE DEBUG INFO WARN ERROR FATAL/;

sub initialize {
    my ($self, $host, $port, @ARGV) = @_;

    my $user; my $passwd; my $raw;
    my $help;

    my $level = shift @ARGV;

    my $res = GetOptions ("help"    => \$help,
			  "raw"     => \$raw,
			  "user=s"   => \$user,
			  "passwd=s" => \$passwd,
	)
	or die "Error in command line arguments";

    if($help) {
	usage();
    }

    die "Error, no user or password"
	unless($user && $passwd);

    $level = uc $level;

    unless($level ~~ @logging_levels) {
	print "Unrecognized logging level : '$level'\n";
	usage();
    }

    my $client = MetaScheduler::Client->new();
    $client->connect($host, $port);

    my $str = buildReq($user, $passwd, $level);

    my $ret = $client->send_req($str);

    if($raw) {
	print $ret;
    } else {
	printResults($ret);
    }
}

sub usage {
    print "Usage: $0 logging [DEBUG|INFO|WARN|ERROR|FATAL] [--raw|-r]\n";
    exit; 
}

sub buildReq {
    my $user = shift;
    my $passwd = shift;
    my $level = shift;

    my $str = "{\n \"version\": \"$protocol_version\",\n";
    $str .= " \"userid\": \"$user\",\n";
    $str .= " \"password\": \"$passwd\",\n";
    $str .= " \"action\": \"logging\",\n";
    $str .= " \"level\": \"$level\"\n";
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

    # Print the message
    print "$res->{msg}\n";

}

1;
