=head1 NAME

    MetaScheduler::Server

=head1 DESCRIPTION

    Object for the TCP server process

=head1 SYNOPSIS

    use MetaScheduler::Server;

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    July 12, 2013

=cut

package MetaScheduler::Server;

use strict;
use warnings;
use MooseX::Singleton;
use POSIX;
use IO::Select;
use IO::Socket;
use Net::hostent;      # for OOish version of gethostbyaddr
use Fcntl;
use Tie::RefHash;
#use Fcntl qw/F_GETFL, F_SETFL, O_NONBLOCK/;
use Fcntl qw/O_NONBLOCK/;
use MetaScheduler::Config;

my $logger;
my $cfg;
my $port;
my $server;
my $sel;
# begin with empty buffers
my %inbuffer  = ();
my %outbuffer = ();
my %ready     = ();

tie %ready, 'Tie::RefHash';

sub initialize {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

    $cfg =  MetaScheduler::Config->config;

    $port = $cfg->{tcp_port} || 7709;

    $server = IO::Socket::INET->new( Proto     => 'tcp',
				     LocalPort => $port,
				     Listen    => SOMAXCONN,
				     Reuse     => 1);
    die "Error, can not create tcp socket" unless($server);

    # Set the server to non-blocking
    $self->nonblock($server);

    # Add the server to the select
    $sel = IO::Select->new($server);

}

sub instance {
    my $self = shift;

    return $self;
}

sub reqs_waiting {
    my $self = shift;
    my $timeout = shift || 1;

    return 1 if($sel->can_read($timeout));

    return 0;
}

sub process_requests {
    my $self = shift;
    my $callback = shift;
    my $client;
    my $rv;
    my $data;

    # anything to read or accept?
    foreach my $client ($sel->can_read(1)) {
	
	if($client == $server) {
	    # accept a new connection
	    
	    $client = $server->accept();
	    $sel->add($client);
	    $self->nonblock($client);
	} else {
	    # read data
	    $data = '';
	    $rv = $client->recv($data, POSIX::BUFSIZ, 0);

	    unless (defined($rv) && length $data) {
		# This would be the end of file, so close the client
                delete $inbuffer{$client};
                delete $outbuffer{$client};
                delete $ready{$client};

                $sel->remove($client);
                close $client;
		$logger->debug("Closing socket");
                next;
	    }

	    $inbuffer{$client} .= $data;

	    # test whether the data in the buffer or the data we
            # just read means there is a complete request waiting
            # to be fulfilled.  If there is, set $ready{$client}
            # to the requests waiting to be fulfilled.
            while ($inbuffer{$client} =~ s/(.*\n)//) {
                push( @{$ready{$client}}, $1 );
            }
	}
    }

    # Any complete requests to process?
    foreach $client (keys %ready) {
        $self->handle($client, $callback);
    }

    # Buffers to flush?
    foreach $client ($sel->can_write(0.1)) {
        # Skip this client if we have nothing to say
        next unless exists $outbuffer{$client};

        $rv = $client->send($outbuffer{$client}, 0);
        unless (defined $rv) {
            # Whine, but move on.
            $logger->warn("I was told I could write, but I can't.");
            next;
        }
        if ($rv == length $outbuffer{$client} ||
            $! == POSIX::EWOULDBLOCK) {
            substr($outbuffer{$client}, 0, $rv) = '';
            delete $outbuffer{$client} unless length $outbuffer{$client};
        } else {
            # Couldn't write all the data, and it wasn't because
            # it would have blocked.  Shutdown and move on.
            delete $inbuffer{$client};
            delete $outbuffer{$client};
            delete $ready{$client};

            $sel->remove($client);
            close($client);
	    $logger->debug("Closing socket, error?");
            next;
        }
    }

    # Out of band data?
    foreach $client ($sel->has_exception(0)) {  # arg is timeout
        # Deal with out-of-band data here, if you want to.
	$logger->error("Error, we're being asked to process out of band data, this shouldn't happen.");
    } 
}

sub handle {
    my $self = shift;
    my $client = shift;
    my $callback = shift;

    foreach my $request (@{$ready{$client}}) {
        # $request is the text of the request
        # put text of reply into $outbuffer{$client}

	# Some sanity checking on what we receive?
	my $results = $callback->process_request($request);
	$outbuffer{$client} = $results;
    }
    delete $ready{$client};
}

sub nonblock {
    my $self = shift;
    my $sock = shift;
    my $flags;

    $flags = fcntl($sock, F_GETFL, 0)
            or die "Can't get flags for socket: $!\n";
    fcntl($sock, F_SETFL, $flags | O_NONBLOCK)
            or die "Can't make socket nonblocking: $!\n";
}

# Check the sockets are still alive and remove 
# them from the select (and socks array) if they've
# closed

sub check_socks {
    my $self = shift;

    foreach my $sock ($sel->handles) {
	# We don't want to check the server in this context
	next if($sock == $server);

	if($sock->connected ~~ undef) {
	    # Hmm, the socket seems to have gone away...

	    delete $inbuffer{$sock};
	    delete $outbuffer{$sock};
	    delete $ready{$sock};

	    $sel->remove($sock);
	    close $sock;
	}
    }

#    for(my $i = $#socks; $i > -1; $i--) {
	# Socket has gone away
#	if($socks[$i]->connected ~~ undef) {
#	    # Remove the socket from the select & array
#	    $sel->remove($socks[$i]);
#	    splice @socks, $i, 1;
#	}
#    }
}

1;
