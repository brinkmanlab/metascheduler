=head1 NAME

    MetaScheduler::Client::admin

=head1 DESCRIPTION

    Object for handling admin requests to the scheduler
    Should be called from the client script and gets it's
    configuration from the @ARGV parameters

=head1 SYNOPSIS

    use MetaScheduler::Client::admin;
    MetaScheduler::Client::admin->initialize($host, $port, @ARGV);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    September 5, 2013

=cut

package MetaScheduler::Client::admin;

use strict;
use warnings;
use JSON;
use MetaScheduler::Client;

use Getopt::Long;

my $protocol_version = '1.0';

my $cmds = { 
    reset => 1,
};

sub initialize {
    my ($self, $host, $port, @ARGV) = @_;

    my $user; my $passwd; my $raw;
    my $help;

    my $subcmd = shift @ARGV;
    my $taskid = shift @ARGV;

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

    unless($cmds->{$subcmd}) {
	print "Unrecognized admin command: $subcmd\n";
	usage();
    }

    unless($taskid =~ /\d+/) {
	print "Unrecognized task_id: $taskid\n";
	usage();
    }

    my $client = MetaScheduler::Client->new();
    $client->connect($host, $port);

    my $str = buildReq($user, $passwd, $subcmd, $taskid);

    my $ret = $client->send_req($str);

    if($raw) {
	print $ret;
    } else {
	printResults($ret);
    }
}

sub usage {
    print "Usage: $0 admin [command] [taskid] [--raw|-r]\n";
    exit; 
}

sub buildReq {
    my $user = shift;
    my $passwd = shift;
    my $subaction = shift;
    my $taskid = shift;

    my $str = "{\n \"version\": \"$protocol_version\",\n";
    $str .= " \"userid\": \"$user\",\n";
    $str .= " \"password\": \"$passwd\",\n";
    $str .= " \"action\": \"admin\",\n";
    $str .= " \"subaction\": \"$subaction\",\n";
    $str .= " \"task_id\": \"$taskid\"\n";
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
