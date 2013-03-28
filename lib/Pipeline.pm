=head1 NAME

    Brinkman::MetaScheduler::Pipeline

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

package Brinkman::MetaScheduler::Pipeline;

use strict;
use warnings;
use JSON;
use Data::Dumper;
use Moose;

my $pipeline;

sub read_pipeline {
    my ($self, $pipeline_file) = @_;

    print "Reading pipeline file $pipeline_file\n";

    return -1
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
	return 0;
    }

    return 1;
}

sub dump {
    return unless($pipeline);

    print Dumper $pipeline;
}

1;
