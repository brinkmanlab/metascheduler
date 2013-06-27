#!/usr/bin/perl

use strict;
use Date::Parse;

my $time = str2time("Wed Apr 10 11:06:43 2013");
print "Time: $time\n";
print localtime($time) . "\n";
my $realtime = time;
print "Real: $realtime\n";
