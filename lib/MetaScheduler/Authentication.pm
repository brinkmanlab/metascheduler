=head1 NAME

    MetaScheduler::Authentication

=head1 DESCRIPTION

    Object to process requests, typically received over
    the TCP server module

=head1 SYNOPSIS

    use MetaScheduler::Authentication;

    MetaScheduler::Authentication->initialize;

    my $authorized = 
       MetaScheduler::Authentication->authenticate($action, $userid, $passwd);

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    August 6, 2013

=cut

package MetaScheduler::Authentication;

use strict;
use warnings;
use MooseX::Singleton;
use MetaScheduler::DBISingleton;
use Log::Log4perl;

my $logger;

sub initialize {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger;

    return $self;
}

sub authenticate {
    my $self = shift;
    my $action = shift;
    my $userid = shift;
    my $passwd = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    # Do authentication here, for now we're just returning true

    return 1;

}

1;
