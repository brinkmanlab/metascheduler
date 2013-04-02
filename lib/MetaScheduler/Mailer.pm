=head1 NAME

    MetaScheduler::Mailer

=head1 DESCRIPTION

    Object for holding and managing emails for jobs

=head1 SYNOPSIS

    use MetaScheduler::Mailer;

=head1 AUTHOR

    Matthew Laird
    Brinkman Laboratory
    Simon Fraser University
    Email: lairdm@sfu.ca

=head1 LAST MAINTAINED

    March 27, 2013

=cut

package MetaScheduler::Mailer;

use strict;
use warnings;
use Moose;
use MetaScheduler::DBISingleton;
use MetaScheduler::Config;
use Log::Log4perl;
use Mail::Mailer;
use Email::Valid;

has saved_emails => (
    traits => ['Array'],
    is    => 'ro',
    isa   => 'ArrayRef[Array]',
    default => sub { [] },
    handles => {
       next_email  => 'shift',
    }
);

has task_id => (
    is    => 'ro',
    isa   => 'Int'
);

my $logger;

sub BUILD {
    my $self = shift;
    my $args = shift;

    $logger = Log::Log4perl->get_logger;

    die "Error, no task_id" unless($args->{task_id});

    # We received an array of emails
    if($args->{emails}) {
	$logger->debug("Adding new emails for task $args->{task_id}: @{$args->{emails}}");
	$self->add_email($args->{task_id}, @{$args->{emails}});
	return;
    }

    # Otherwise we're just loading a set of emails from the database
    $logger->debug("Loading existing email set for task $args->{task_id}");
    $self->load_emails($args->{task_id});

}

sub add_email {
    my $self = shift;
    my $task_id = shift;
    my @new_emails = @_;

    # Add to db and get back mail_id
    my $dbh = MetaScheduler::DBISingleton->dbh;
  
    my $sqlstmt = qq{INSERT INTO mail (task_id, email) VALUES (?, ?)};
    my $add_email = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $logger->debug("Locking tables for add_email, task_id $task_id");
    $dbh->do("LOCK TABLES mail WRITE");

    # Go through the emails we were given
    EMAIL: foreach my $email (@new_emails) {

	unless(Email::Valid->address($email)) {
	    $logger->error("Invalid email address for task_id $task_id, email $email");
	    next EMAIL;
	}

	# Add each email to the database
	$logger->debug("Adding email task_id $task_id, email $email");
	$add_email->execute($task_id, $email) or
	    die "Error inserting email ($task_id, $email): $DBI::errstr";

	my $mail_id = $dbh->last_insert_id( undef, undef, undef, undef );

	die "Error, no mail_id returned ($task_id, $email)"
	    unless($mail_id);

	# And record this email structure for later use in our
	# internal attribute
	# Format: [mail_id, email, sent, date_sent]
	my $one_email = [$mail_id, $email, 0, 0];
	push @{ $self->saved_emails }, $one_email;
    }

    $logger->debug("Unlocking tables for add_email, task_id $task_id");
    $dbh->do("UNLOCK TABLES");
}

sub load_emails {
    my $self = shift;
    my $task_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT mail_id, email, sent, UNIX_TIMESTAMP(sent_date) AS epoch FROM mail WHERE task_id = ?};
    my $fetch_emails =  $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $logger->debug("Loading mailer task_id $task_id");
    $fetch_emails->execute($task_id) 
	or die "Error executing statement $sqlstmt: $DBI::errstr";

    while(my @row = $fetch_emails->fetchrow_array) {
	my $one_email = [$row[0], $row[1], $row[2], $row[3]];
	push @{ $self->saved_emails }, $one_email;
    }
}

sub fetch_emails {
    my $self = shift;

    # return array [[email, sent, sent_date, mail_id], [],...]
    return $self->saved_emails;
}

# Possible arguments
# { all => 0/1,
#   msg => "str",
#   mail_id => int
# }
# Nah, when would we only need to send some?
sub send_email {
    my $self = shift;
    my $args = shift;

    my $task_id = $self->task_id;
    $logger->debug("Sending emails for task_id $task_id");

    # Do email sending stuff later
    my $mailer = Mail::Mailer->new();

#    foreach my $email_obj (@{$self->saved_emails}) {
    EMAIL: while (my $email_obj = $self->next_email()) {
	my $email = $email_obj->[1];
	next EMAIL unless($args->{resend} || !( $email_obj->[2]));
	$logger->debug("Sending email for task_id $task_id, email $email");
	$mailer->open({ From    => $args->{from},
			To      => $email,
			Subject => $args->{subject},
		      })
	    or die "Error, can't open email: $!";

	print $mailer $args->{msg};
	$mailer->close();
    }
    
    my $dbh = MetaScheduler::DBISingleton->dbh; 

    $dbh->do("UPDATE mail SET sent = 1, sent_date = NOW() WHERE task_id = $task_id");

    # We do it this way to we get the exact NOW() that
    # mysql set for the rows
    $logger->debug("Reloading task $task_id");
    $self->load_emails($task_id);

}

1;
