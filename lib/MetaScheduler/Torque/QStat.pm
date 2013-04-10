package MetaScheduler::Torque::QStat;

use strict;
use warnings;
use Data::Dumper;
use Log::Log4perl;
use MooseX::Singleton;
use MetaScheduler::Config;

my $logger;

has 'jobs' => (
    traits => ['Hash'],
    is     => 'rw',
    isa    => 'HashRef',
    default   => sub { {} },
    handles   => {
          set_job     => 'set',
          get_job     => 'get',
          delete_job  => 'delete',
	  clear_jobs  => 'clear',
      },

);

has 'last_update' => (
    is     => 'rw',
    isa    => 'Int',
);

sub initialize {
    my $self = shift;

    $logger = Log::Log4perl->get_logger;

    $self->parse_qstat;

}

sub refresh {
    my $self = shift;
    my $args = shift;

    return unless($self->expired || $args->{force});

    $logger->debug("Refreshing jobs list");

    $self->parse_qstat;
}

sub fetch {
    my $self = shift;
    my $jobid = shift;

    $self->refresh;

    return $self->jobs->{$jobid};
}

sub expired {
    my $self = shift;
    
    return 0 unless((time - $self->last_update) >  
		    MetaScheduler::Config->config->{torque_update_interval});

    $logger->debug("QStat cache expired");

    return 1;
}

sub parse_qstat {
    my $self = shift;

    my $config = MetaScheduler::Config->config;

    my $qstat_txt;
    {
	local $/ = ''; # change INPUT_RECORD_SEPARATOR to blank line 
	$logger->debug("Slurping in qstat");
	open my $fh, "$config->{torque_qstat}|"
	    or die "Error slurping qstat: $!";
	while($qstat_txt = <$fh>) {
	    $self->parse_record($qstat_txt);
	}
    }

    $self->last_update(time());

#    $self->clear_jobs;
#    print Dumper $self->jobs;
    
}

sub parse_record {
    my $self = shift;
    my $qstat_rec = shift;
    my $job;

    my @lines = split "\n", $qstat_rec;

    my ($jobid) = (shift @lines) =~ /Job Id: (.+)$/;

    chomp (%$job = map { split /\s*=\s*/,$_,2 } grep (!/^$/,@lines));

#    print "Job: $jobid\n";
#    print Dumper $job;

    $self->set_job($jobid => $job);
}

1;
