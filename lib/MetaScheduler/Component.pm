=head1 NAME

    Component.pm

=head1 DESCRIPTION

    Object for holding components of an individual job

    Not invoked on it's own, an internal object of Job

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    March 27, 2013

=cut

package Component;

use strict;
use warnings;
use Moose;
use Moose::Util::TypeConstraints;
use Carp qw( confess );

has component_id => (
    is     => 'ro',
    isa    => 'Int'
);

has task_id => (
    is     => 'ro',
    isa    => 'Int'
);

has component_type => (
    is     => 'ro',
    isa    => 'Str'
);

has run_status => (
    is     => 'ro',
    isa    => enum([qw(PENDING COMPLETE HOLD ERROR RUNNING)]) );
);

has extra_parameters => (
    is     => 'ro',
    isa    => 'Str'
);

has qsub_file => (
    is     => 'ro',
    isa    => 'Str'
);

has qsub_id => (
    is     => 'ro',
    isa    => 'Int'
);

has start_date  => (
    is     => 'ro',
    isa    => 'Int'
);

has complete_date  => (
    is     => 'ro',
    isa    => 'Int'
);

1;
