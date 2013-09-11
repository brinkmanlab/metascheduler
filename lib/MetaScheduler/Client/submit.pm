=head1 NAME

    MetaScheduler::Client::submit

=head1 DESCRIPTION

    Object for handling submission requests to the scheduler
    Should be called from the client script and gets it's
    configuration from the @ARGV parameters

=head1 SYNOPSIS

    use MetaScheduler::Client::submit;
    MetaScheduler::Client::submit->initialize($host, $port, @ARGV);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    September 5, 2013

=cut

package MetaScheduler::Client::submit;

use strict;
use warnings;
use MetaScheduler::Client;

use Getopt::Long;

my $protocol_version = '1.0';

sub initialize {
    my ($self, $host, $port, @ARGV) = @_;

    my $input; my $user; my $passwd; my $json; my $help;

    my $res = GetOptions ("input=s"  => \$input,
			  "user=s"   => \$user,
			  "passwd=s" => \$passwd,
			  "help"     => \$help,
	)
	or die "Error in command line arguments";

    if($help) {
	usage();
    }

    die "Error, no user or password"
	unless($user && $passwd);

    # Set the file handle based on if we have
    # an input file
    my $fh;
    if($input) {
	die "Error, can't read input file $input"
	    unless( -f $input && -r $input );
	open $fh, "<", $input 
	    or die "Error opening input file $input: $@";
    } else {
	$fh = 'STDIN';
    }

    {
	local $/; # enable slurp
	$json = <$fh>;
	# Don't bother closing, we don't want to close
	# if it's STDIN, and for a file it will automatically
	# close on exit
    }

    my $client = MetaScheduler::Client->new();
    $client->connect($host, $port);

    my $str = buildReq($user, $passwd, $json);

    my $ret = $client->send_req($str);

    print $ret;
}

sub buildReq {
    my $user = shift;
    my $passwd = shift;
    my $json = shift;

    my $str = "{\n \"version\": \"$protocol_version\",\n";
    $str .= " \"userid\": \"$user\",\n";
    $str .= " \"password\": \"$passwd\",\n";
    $str .= " \"action\": \"submit\",\n";
    $str .= " \"record\": ";
    $str .= $json . "\n";
    $str .= " }\nEOF\n";

    return $str;
}

sub usage {
    print "Usage: $0 submit [-s json_file] [--raw|-r]\n";
    exit; 
}

1;
