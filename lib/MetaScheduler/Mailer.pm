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

has emails => (
    is    => 'ro',
    isa   => 'ArrayRef',
    default => sub { [] }
);

has task_id => (
    is    => 'ro',
    isa   => 'Int'
);

sub BUILD {
    my $self = shift;
    my $args = shift;

    die "Error, no task_id" unless($args->{task_id});

    # We received an array of emails
    if($args->{emails}) {
	$self->add_email($args->{task_id}, @{$args->{emails}});
	return;
    }

    # Otherwise we're just loading a set of emails from the database
    $self->load_emails($args->{task_id});
}

sub add_email {
    my $self = shift;
    my $task_id = shift;
    my @emails = @_;

    # Add to db and get back mail_id
    my $dbh = MetaScheduler::DBISingleton->dbh;
  
    my $sqlstmt = qq{INSERT INTO mail (task_id, email) VALUES (?, ?)};
    my $add_email = $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $dbh->do("LOCK TABLES mail WRITE");

    # Go through the emails we were given
    foreach my $email (@emails) {

	# Add each email to the database
	$add_email->execute($task_id, $email) or
	    die "Error inserting email ($task_id, $email): $DBI::errstr";

	my $mail_id = $dbh->last_insert_id( undef, undef, undef, undef );

	die "Error, no mail_id returned ($task_id, $email)"
	    unless($mail_id);

	# And record this email structure for later use in our
	# internal attribute
	# Format: [mail_id, email, sent, date_sent]
	my $one_email = [$mail_id, $email, 0, 0];
	push @{ $self->emails }, $one_email;
    }

    $dbh->do("UNLOCK TABLES");
}

sub load_emails {
    my $self = shift;
    my $task_id = shift;

    my $dbh = MetaScheduler::DBISingleton->dbh;

    my $sqlstmt = qq{SELECT mail_id, email, sent, UNIX_TIMESTAMP(sent_date) AS epoch FROM mail WHERE task_id = ?};
    my $fetch_emails =  $dbh->prepare($sqlstmt) or die "Error preparing statement: $sqlstmt: $DBI::errstr";

    $fetch_emails->execute($task_id) 
	or die "Error executing statement $sqlstmt: $DBI::errstr";

    while(my @row = $fetchrow_array) {
	my $one_email = [$row[0], $row[1], $row[2], $row[3]];
	push @{ $self->emails }, $one_email;
    }
}

sub fetch_emails {
    my $self = shift;

    # return array [[email, sent, sent_date, mail_id], [],...]
    return $self->emails;
}

# Possible arguments
# { all => 0/1,
#   msg => "str",
#   mail_id => int
# }
# Nah, when would we only need to send some?
sub sent_email {
    my $self = shift;
    my $msg = shift;

    # Do email sending stuff later
    
    my $dbh = MetaScheduler::DBISingleton->dbh; 

    my $task_id = $self->task_id;
    $dbh->do("UPDATE mail SET sent = 1, sent_date = NOW() WHERE task_id = $task_id";

}

1;
