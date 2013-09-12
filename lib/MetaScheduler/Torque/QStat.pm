package MetaScheduler::Torque::QStat;

use strict;
use warnings;
use Data::Dumper;
use Date::Parse;
use Log::Log4perl;
use MooseX::Singleton;
use MetaScheduler::Config;

my $logger;

has 'state' => (
    traits => ['Hash'],
    is     => 'ro',
    isa    => 'HashRef',
    default => sub { { 'C' => "Complete",
		 'E' => "Exited",
		 'H' => "Hold",
		 'Q' => 'Queued',
		 'R' => 'Running',
		 'T' => 'Transfer',
		 'W' => 'Waiting',
		 'S' => 'Suspended' }}
);

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
	  job_pairs   => 'kv',
	  num_jobs    => 'count',
      },

);

has 'stats' => (
    traits => ['Hash'],
    is     => 'rw',
    isa    => 'HashRef',
    default  => sub { {} },
    handles  => {
	set_stat    => 'set',
	get_stat    => 'get',
	delete_stat => 'delete',
	clear_stats => 'clear',
	stat_pairs  => 'kv',
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

sub poll {
    my $self = shift;
    my $job_id = shift;

    $self->refresh;

    my $job = $self->fetch($job_id);

    return undef unless($job);

    return $job->{job_state};
}

sub refresh {
    my $self = shift;
    my $args = shift;

    return unless($args->{force} || $self->expired);

    $logger->trace("Refreshing jobs list");

    $self->parse_qstat;
    $self->refresh_stats;
}

# Fetch an individual job from the list

sub fetch {
    my $self = shift;
    my $jobid = shift;

    $self->refresh;

    # Append the server name if we're only given a numeric id
    $jobid = "$jobid." . MetaScheduler::Config->config->{torque_server_name} 
        if($jobid =~ /^(\d+)$/);

    return $self->jobs->{$jobid};
}

# Check if too much time has passed and we
# want to expire the cache

sub expired {
    my $self = shift;
    
    return 0 unless((time - $self->last_update) >  
		    MetaScheduler::Config->config->{torque_update_interval});

    $logger->debug("QStat cache expired");

    return 1;
}

# Returns true if we're full for new jobs
# False if there's room for more

sub scheduler_full {
    my $self = shift;

    my $c = 0;

    for my $stats ($self->stat_pairs) {
	# We don't want to count completed jobs
	next if($stats->[0] eq "E");

	$c += $stats->[1];
    }

    # Return if the number of jobs is too many
    return ($c > MetaScheduler::Config->config->{torque_max_jobs});
}

sub parse_qstat {
    my $self = shift;

    my $config = MetaScheduler::Config->config;

    my $qstat_txt;
    {
	local $/ = ''; # change INPUT_RECORD_SEPARATOR to blank line 
	$logger->trace("Slurping in qstat");
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

    # Remove leading and trailing spaces
    s{^\s+|\s+$}{}g foreach @lines;

    my ($jobid) = (shift @lines) =~ /Job Id: (.+)$/;

    chomp (%$job = map { split /\s*=\s*/,$_,2 } grep (!/^$/,@lines));

#    print "Job: $jobid\n";
#    print Dumper $job;

    # We want to parse out jobs that aren't ours
    return unless($job->{Job_Name} =~ /^MetaS/);

    $self->set_job($jobid => $job);
}

sub refresh_stats {
    my $self = shift;

    $self->clear_stats;
    $self->expire_old_jobs;

    for my $job ($self->job_pairs) {
	# Increment the stat that this job is in
	$self->inc($job->[1]->{job_state});
    }

    $self->dump_stats;

}

# Expire older completed jobs

sub expire_old_jobs {
    my $self = shift;

    for my $job ($self->job_pairs) {
	# We only care about jobs that are in the complete state
	next unless($job->[1]->{job_state} eq 'E');

	next unless((time - str2time($job->[1]->{mtime})) >  
		    MetaScheduler::Config->config->{torque_expire_time});

	$logger->debug("Expiring completed job $job->[0], mtime was $job->[1]->{mtime}");
	$self->delete_job($job->[0]);
    }

}

sub dump_stats {
    my $self = shift;

    for my $stat ($self->stat_pairs) {
	print "$stat->[0] : $stat->[1]\n";
    }
}

sub inc {
    my $self      = shift;
    my $key       = shift;
    my $increment = shift || 1;

    my $value = $self->get_stat($key) || 0;

    # bail out if value != numeric
    if($value !~ m/^\d+$/) {
        return $value;
    }

    $value += $increment;
    $self->set_stat( $key, $value );

    return $value;
}

sub dec {
    my $self      = shift;
    my $key       = shift;
    my $decrement = shift || 1;

    my $value = $self->get_stat($key) || 0;

    # bail out if value != numeric
    if($value !~ m/^\d+$/) {
        return $value;
    }

    $value -= $decrement;
    $self->set_stat( $key, $value );

    return $value;
}


1;
