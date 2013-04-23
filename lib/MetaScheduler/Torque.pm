=head1 NAME

    MetaScheduler::Torque

=head1 DESCRIPTION

    Interface to Torque/PBS

    Not invoked on it's own, an internal object of Pipeline

    Maybe should be subclassed from something more general
    to create a standardized API to different schedulers? (bugaboo?)

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    April 4, 2013

=cut

package MetaScheduler::Torque;

use strict;
use warnings;
use Log::Log4perl;
use MooseX::Singleton;
use MetaScheduler::Torque::QStat;

my $logger;

sub initialize {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

    MetaScheduler::Torque::QStat->initialize;
}

sub instance {
    my $self = shift;

    return $self;
}

1;
