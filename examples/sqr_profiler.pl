#!/usr/local/bin/perl
#
# Copyright (c) 2003 William Goedicke. All rights reserved. This
# program is free software; you can redistribute it and/or modify it
# under the same terms as Perl itself.

use strict;
use PeopleSoft::Tools;

my ( %opts, $filespec, $use_mode, $letter );
my ( $buf );
my $G_calls = new Graph;

usage();

if ( $use_mode eq "p" ) {
  open INFILE, "< $filespec" || die "Couldn't open $filespec";
}
else {
  my $orig_filespec = $filespec . "_orig";
  if ( open TESTFILE, $orig_filespec ) {
    close TESTFILE;
    die "You've already got a $orig_filespec; I won't overwrite it";
  }
  rename $filespec, $orig_filespec || die "Couldn't rename $filespec to $orig_filespec";
  open OUTFILE, "> $filespec" || die "Couldn't create new copy of $filespec";
  open INFILE, "< $orig_filespec" || die "Couldn't open $orig_filespec";
}

while(<INFILE>) { $buf .= $_; }

if ( $use_mode eq "m" ) {
  print OUTFILE munge($buf, $letter);
}
elsif ( $use_mode eq "p" ) {
  my $hbuf = profile($buf);
  print $hbuf;
}
elsif ( $use_mode eq "u" ) {
  my $ubuf = unmunge($buf);
  print OUTFILE $ubuf;
}

exit;

# --------------------------------------------------- Print usage
sub usage {
  getopts('ivf:m:', \%opts);

  if ( ! defined $opts{'m'} or
       ! defined $opts{'f'} ) {
    print <<USAGE;

This script provides a profiling mechanism for SQRs.  You first run it
in "munge" mode to add logging statements to the specified SQRs.  Then
execute the SQRs create log/profiling data.  Once the logs are created
you run the script again in "profile" mode and it generates an HTML
table of the cumulative time each subroutine, DDL and DML took to
execute.  There is also an "unmunge" mode to remove the profiling
statements from an SQR.

  Usage: sqr_profiler.pl [ -l <letter> ] -m m -f <filespec>
         sqr_profiler.pl -m p -f <filespec>
         sqr_profiler.pl -m u -f <filespec>

  -m {p|m|u}      - Mode: p for profile, m for munge, u for unmunge
  -l <letter>     - Letter for debug statement (default p)
  -f <filespce>   - Specify source file(s) to munge or log file to analyze

USAGE

    exit 1;
  }
  if ( defined $opts{'l'} ) { $letter = $opts{'l'}; }
  else                      { $letter = "p"; }

  $filespec = $opts{'f'};
  $use_mode = $opts{'m'};
}
