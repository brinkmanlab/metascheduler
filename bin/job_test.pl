#!/usr/bin/env perl

use warnings;
use strict;

print "Returning a status of success\n";
print "Received arguments ";
foreach my $arg (0 .. $#ARGV) {
    print "[$ARGV[$arg]] ";
}
print "\n";

exit 0;
