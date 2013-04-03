=head1 NAME

    MetaScheduler::Pipeline

=head1 DESCRIPTION

    Object for holding and managing individual pipelines

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    March 27, 2013

=cut

package MetaScheduler::Pipeline;

use strict;
use warnings;
use JSON;
use Data::Dumper;
use Log::Log4perl;
use Graph::Directed;
use Moose;

my $pipeline;
my $logger;
my $g;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger;

    die "Error, no pipeline file given" unless($args->{pipeline});

    $self->read_pipeline($args->{pipeline});

    $self->build_tree;
}

sub read_pipeline {
    my ($self, $pipeline_file) = @_;

    $logger->debug("Reading pipeline file $pipeline_file\n");

    die "Error, pipeline file not readable: $pipeline_file"
	unless ( -f $pipeline_file && -r $pipeline_file );

    my $json;
    {
	local $/; #enable slurp
	open my $fh, "<", "$pipeline_file";
	$json = <$fh>;
    } 

    eval {
	$pipeline = decode_json($json);
    };
    if($@) {
	# Error decoding JSON
	die "Error, can not decode pipeline file $pipeline_file: $!";
    }

}

sub build_tree {
    my ($self) = @_;

    $logger->debug("Building the graph for the pipeline");
    $g = Graph::Directed->new;

    foreach my $component (@{$pipeline->{'components'}}) {
	my $name = $component->{name};
	$logger->debug("Adding component $name");
	$self->add_edges($name, $component->{on_success}, 'success')
	    if($component->{on_success});

	$self->add_edges($name, $component->{on_failure}, 'failure')
	    if($component->{on_failure});

    }
    
    print "Graph:\n$g\n";

}

sub add_edges {
    my ($self, $origin, $vertices, $label) = @_;

    $logger->debug("Adding edges for component $origin, label $label, verticies $vertices");

    foreach my $d (split ',', $vertices) {
	$g->add_edge($origin, $d);
	$g->set_edge_attribute($origin, $d, 'result', $label);
    }
}

sub fetch_component {
    my ($self, $name) = @_;

    die "Error, no pipeline loaded"
	unless($pipeline);

    $logger->debug("Returning pipeline component $name");

    foreach my $component (@{$pipeline->{'components'}}) {
	return $component
	    if($name eq $component->{'name'});
    }

    return 0;
}

sub dump {
    return unless($pipeline);

    print Dumper $pipeline;
}

1;
