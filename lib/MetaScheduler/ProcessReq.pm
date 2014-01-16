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
    graph  => 'graph',
    admin  => 'admin',
    logging => 'logging',
    refresh => 'refresh',
};

my @logging_levels = qw/DEBUG INFO WARN ERROR FATAL/;

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
	$logger->error("Error decoding submitted json:\n $json");
	return (400, "{ \"code\": \"400\",\n\"msg\": \"Error decoding JSON\" }");
    }

    # Does the job have a valid action?
    unless($json->{action} && $actions->{$json->{action}}) {
	$logger->error("Error, no valid action was submitted: " . $json->{action});
	return (400, "{ \"code\": \"400\",\n\"msg\": \"Error, no valid action submitted\" }");
    }

    # Authenticate the request
    unless(MetaScheduler::Authentication->authenticate($json->{action},
						       ($json->{userid} ? $json->{userid} : undef),
						       ($json->{passwd} ? $json->{passwd} : undef))) {
	
	$logger->warn("Not authorized, action: " . $json->{action} . ", userid: " . ($json->{userid} ? $json->{userid} : 'none'));

	    return (401, "{ \"code\": \"400\",\n\"msg\": \"Not authorized, action: " . $json->{action} . "\" }");
    }
	   
    # Dispatch the request
    my $action = $json->{action};
    my ($ret_code, $ret_json);
    eval {
    ($ret_code, $ret_json) = $self->$action($callback, $json);
    };

    if($@) {
	$logger->error("Error dispatching action $action: $@");
	return (500, $self->makeResStr(500, "Error dispatching action $action"));
    }

    return ($ret_code, $ret_json);
}

# For testing, manually call a dispatch table entry

sub dispatch {
    my $self = shift;
    my $action = shift;
    my $args = shift;

    $logger->debug("Requested action: $action\n");

    return $self->$action($args);
}

sub submit {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    print Dumper $args;

    my $res;
    if($res = $callback->addJob($args->{record})) {
	return (200, $self->makeResStr(200, "Job submitted, job id: $res"));
    }

    return (500, $self->makeResStr(500, "Error, job could not be submitted"));
}

sub show {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    print Dumper $args;

    my ($records, $res) = $callback->showJob(($args->{type} ? $args->{type} : 0));
    if($records) {
	return (200, $self->makeResStr(200, "Success, jobs returned: $records", $res));
    }
    
    return (404, $self->makeResStr(404, "No jobs returned"));
}

sub alter {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    if($args->{task_id} && $args->{change}) {
	my $res = $callback->alterJob($args->{task_id}, $args->{change}, $args->{component});

	my $comp_str = '';
	$comp_str = " (component: $args->{component})" if($args->{component});
	return (200, $self->makeResStr(200, "Successful, task id " . $args->{task_id} . "$comp_str set to " . $args->{change})) if($res);
    }

    return (500, $self->makeResStr(404, "Error, unable to set state"));

}

sub delete {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    if($args->{task_id}) {
	my $res = $callback->alterJob($args->{task_id}, 'delete');

	return (200, $self->makeResStr(200, "Successful, task id " . $args->{task_id} . " deleted")) if($res);
    }

    return (500, $self->makeResStr(404, "Error, unable to delete task"));

}

sub status {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    if($args->{task_id}) {
	my $res = $callback->statusJob($args->{task_id});

	return (200, $self->makeResStr(200, $res)) if ($res);
    }

    return (404, $self->makeResStr(404, "Error, task not found"));

}

sub graph {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    if($args->{task_id}) {
	my $res = $callback->graphJob($args->{task_id});

	return (200, $self->makeResStr(200, $res)) if ($res);
    }

    return (404, $self->makeResStr(404, "Error, graph not found"));
}

sub admin {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    my $res = 0;

    if($args->{task_id}) {
	if($args->{subaction} eq "reset") {
	    $res = $callback->resetJob($args->{task_id});
	} else {
	    return (500, $self->makeResStr(500, "Unknown action"));
	}

	return (200, $self->makeResStr(200, "Success")) if ($res);
    }

    return (500, $self->makeResStr(500, "Unknown in admin command"));

}

sub logging {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    my $res = 0;

    if($args->{level} ~~ @logging_levels) {
	$res = $callback->alterLogLevel($args->{level});
    } else {
	return (500, $self->makeResStr(500, "Unknown log level"));
    }

    return (200, $self->makeResStr(200, "Success")) if ($res);

    return (500, $self->makeResStr(500, "Unknown error"));
}

sub refresh {
    my $self = shift;
    my $callback = shift;
    my $args = shift;

    my $res = 0;

    $res = $callback->refreshSchedulers();

    return (200, $self->makeResStr(200, "Success")) if ($res);

    return (500, $self->makeResStr(500, "Unknown error"));
}

sub makeResStr {
    my $self = shift;
    my $code = shift;
    my $msg = shift;
    my $res = shift;

    my $ret_json = "{\n \"code\": \"$code\",\n \"msg\": \"$msg\"\n";
    if($res) {
	$ret_json .= ", \"results\": $res\n";
    }
    $ret_json .= "}\n";

    return $ret_json;
}

1;
