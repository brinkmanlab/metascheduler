=head1 NAME

    MetaScheduler::Client

=head1 DESCRIPTION

    Object for the TCP client process

=head1 SYNOPSIS

    use MetaScheduler::Client;

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    July 15, 2013

=cut

package MetaScheduler::Client;

use strict;
use warnings;
use MooseX::Singleton;
use POSIX;
use IO::Select;
use IO::Socket;
use Net::hostent;      # for OOish version of gethostbyaddr
use Fcntl;
use Tie::RefHash;
use Fcntl qw/O_NONBLOCK/;
#use Fcntl qw/F_GETFL, F_SETFL, O_NONBLOCK/;
use MetaScheduler::Config;
use Log::Log4perl(get_logger :nowarn);

my $logger;
my $cfg;
my $handle;
my $alarm_timeout = 60;
my $protocol_version = '1.0';

sub BUILD {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

    $cfg =  MetaScheduler::Config->config;

}

sub connect {
    my $self = shift;
    my $host = shift;
    my $port = shift || 7709;

    $handle = IO::Socket::INET->new(Proto     => "tcp",
				    PeerAddr  => $host,
				    PeerPort  => $port)
       or die "can't connect to port $port on $host: $!";

    $logger->debug("Connected to $host:$port");
}

sub send_req {
    my $self = shift;
    my $msg = shift;
    my $received = '';


    # Check if the message ends with a LF, if not
    # add one
    $msg .= "\n" unless($msg =~ /\n$/);
    my $length = length $msg;

    # Make sure the socket is still open and working
    if($handle->connected ~~ undef) {
	$logger->error("Error, socket seems to be closed");
	return;
    }

    # Set up an alarm, we don't want to get stuck
    # since we are allowing blocking in the send
    # (the server might not be ready to receive, it's
    # not multi-threaded, just multiplexed)
    eval {
	local $SIG{ALRM} = sub { die "timeout\n" };
	alarm $alarm_timeout;
	
	# While we still have data to send
	while($length > 0) {
	    my $rv = $handle->send($msg, 0);

	    # Oops, did we fail to send to the socket?
	    unless(defined $rv) {
		$logger->error("We weren't able to send to the socket for some reason.");
		# Turn the alarm off!
		alarm 0;
		return undef;
	    }

	    # We've sent some or all of the buffer, record that
	    $length -= $rv;

	}

	# The message is sent, now we wait for a reply, or until
	# our alarm goes off
	while($received !~ /\n$/) {
	    my $data;
	    # Receive the response and put it in the queue
	    my $rv = $handle->recv($data, POSIX::BUFSIZ, 0);
	    unless(defined($rv)) {
		$logger->error("We didn't receive anything back on the socket");
		alarm 0;
		return undef;
	    }
	    $received .= $data;
	}

	# We've successfully made our request, clear the alarm
	alarm 0;
    };
    # Did we get any errors back?
    if($@) {
	# Uh-oh, we had an alarm, the iteration timed out
	if($@ eq "timeout\n") {
	    $logger->error("Error sending request, the alarm went off, timeout!");
	    return undef;
	} else {
	    $logger->error("Error sending request: " . $@);
	    return undef;
	}
    }

    # Success! Return the results
    return $received;
}

1;
