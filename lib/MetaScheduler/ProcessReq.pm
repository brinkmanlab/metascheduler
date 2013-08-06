=head1 NAME

    MetaScheduler::ProcessReq

=head1 DESCRIPTION

    Object to process requests, typically received over
    the TCP server module

=head1 SYNOPSIS

    use MetaScheduler::ProcessReq;

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    August 6, 2013

=cut

package MetaScheduler::ProcessReq;

use strict;
use warnings;
use MooseX::Singleton;
use MetaScheduler::DBISingleton;
use MetaScheduler::Authentication;
use JSON;
use Log::Log4perl;
use Data::Dumper;

my $actions = {
    submit => 'submit',
    show   => 'show',
    alter  => 'alter',
    delete => 'delete',
    status => 'status',
    admin  => 'admin',
};

my $logger;

sub initialize {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger;

    return $self;
}

sub process_request {
    my $self = shift;
    my $callback = shift;
    my $req = shift;

    my $json;

    eval {
	$json = decode_json($req);
    };
    if($@) {
	$logger->error("Error decoding submitted json");
	return (400, "{ \"code\": \"400\",\n\"Msg\": \"Error decoding JSON\" }");
    }

    # Does the job have a valid action?
    unless($json->{action} && $actions->{$json->{action}}) {
	$logger->error("Error, no valid action was submitted: " . $json->{action});
	return (400, "{ \"code\": \"400\",\n\"Msg\": \"Error, no valid action submitted\" }");
    }

    # Authenticate the request
    unless(MetaScheduler::Authentication->authenticate($json->{action},
						       ($json->{userid} ? $json->{userid} : undef),
						       ($json->{passwd} ? $json->{passwd} : undef))) {
	
	$logger->warn("Not authorized, action: " . $json->{action} . ", userid: " . ($json->{userid} ? $json->{userid} : 'none'));

	    return (401, "{ \"code\": \"400\",\n\"Msg\": \"Not authorized, action: " . $json->{action} . "\" }");
    }
	   
    # Dispatch the request
    my $action = $json->{action};
    eval {
    $self->$action($callback, $json);
    };

    if($@) {
	$logger->error("Error dispatching action $action: $@");
    }
}

# For testing, manually call a dispatch table entry

sub dispatch {
    my $self = shift;
    my $action = shift;
    my $args = shift;

    print "Requested action: $action\n";

    $self->$action($args);
}

sub submit {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    print Dumper $args;

    my $res;
    if($res = $callback->addJob($args->{record})) {
	
    }
}

1;
