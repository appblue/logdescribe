#!/usr/bin/env perl -w
#
#   author  : Krzysztof Kielak <krzysztof.kielak@gmail.com>
#   version history
#     0.1 : initial version with log parser functionality
#     0.2 : added simple log fields analysis
#
#   Tool for log parsing & processing
#

use strict;

# constants
my $RAWLOG     = 0x01;
my $MAX_UNIQUE = 10;

# timestamp management
my $ts_fid  = undef;                                   # timestamp field number
my $ts_fmt  = '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}';  # ex: 2015-02-15 20:34:59
my @ts_chg  = ();
my %ts_eps  = ();
my %ts_data = ();

# global structure that hold information about log fields
my @fstat = ();

# global variable holding currently processed logline
my $log_entry = "";

sub banner($) {
    my $line = shift @_;
    my $h    = "-" . " " x 10 . $line . " " x 10 . "-";
    print "\n";
    print "-" x ( length $h ) . "\n";
    print $h . "\n";
    print "-" x ( length $h ) . "\n\n";
}

sub find_timestamp($$) {
    my $l     = shift @_;
    my $lnum  = shift @_;         # line number in log
    my @field = split /\|/, $l;

    for ( my $i = 0 ; $i < $#field + 1 ; $i++ ) {
        if ( $field[$i] =~ /$ts_fmt/ ) {
            push @ts_chg, $lnum if ( defined $ts_fid ) && ( $ts_fid != $i );
            $ts_fid = $i;
        }
    }
}

sub report_ts_field() {
    banner "TIMESTAMP INFO";
    printf "%10s : Timestamp was detected in Field : %2d\n", "[INFO]",
      $ts_fid + 1
      if defined $ts_fid;
    printf "%10s : Timestamp was not detected.\n", "[WARNING]"
      if !defined $ts_fid;
    if ( $#ts_chg >= 0 ) {
        printf "%10s : Timestamp field changed betweet log entries.\n",
          "[WARNING]";
        printf "%10s : %s %d\n", "[WARNING]",
          "  Timestamp field has changed in line no", $_
          for (@ts_chg);
    }
}

sub print_log_entry($$) {
    my $l     = shift @_;
    my $flag  = shift @_;
    my @field = split /\|/, $l;
    my $cnt   = 1;

    if ( $flag & $RAWLOG ) {
        my $h = "-" x 10 . " R_A_W  L_O_G " . "-" x 10;
        print "$h\n\n";
        print $l;
        print "\n\n" . "-" x ( length($h) ) . "\n\n";
    }

    for my $i (@field) {
        printf( "%03d: %s\n", $cnt++, $i );
    }
    print "\n";
}

#
# Process stream of lines from a log file. Funtion returns:
#   0 - if log entry in a buffer is not ready for processing
#   1 - if log entry stored in $log_entry buffer is ready for processing
#
sub process_log_entry($) {
    my $l = shift @_;

    chomp $l;

    # remove \r lines from the logfile
    $l =~ s/\r//g;

    if ( $l =~ /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\|[A-Z]+\|/ ) {
        my $retval = $log_entry;
        $log_entry = $l;
        return $retval;
    }
    else {
        $log_entry .= "\\n" . $l;
    }
    return 0;
}

sub analyze_log_entry($) {
    my $l = shift @_;
    my @field = split /\|/, $l;

    # trim timestamp field to seconds and update data in %ts_eps and %ts_data
    if ( $field[$ts_fid] =~ /$ts_fmt/ ) {
        $ts_eps{$&}++;
        $ts_data{$&} += length($l);
    }

    for ( my $i = 0 ; $i < $#field + 1 ; $i++ ) {

        # name the fields from 001
        $fstat[$i]{'name'} = $i + 1;

        # add information about min and max length of the field values
        $fstat[$i]{'minlen'} = length( $field[$i] )
          if !defined $fstat[$i]{'minlen'};
        $fstat[$i]{'minlen'} = length( $field[$i] )
          if $fstat[$i]{'minlen'} > length( $field[$i] );

        $fstat[$i]{'maxlen'} = length( $field[$i] )
          if !defined $fstat[$i]{'maxlen'};
        $fstat[$i]{'maxlen'} = length( $field[$i] )
          if $fstat[$i]{'maxlen'} < length( $field[$i] );

        # empty field has a special symbol <empty field>
        my $val = $field[$i] ne "" ? $field[$i] : "<empty field>";

        # capture only $MAX_UNIQUE values for the field and map the rest to
        # special symbol '##---other---##'
        if ( scalar keys %{ $fstat[$i]{'v'} } > $MAX_UNIQUE ) {
            $val = '##---other---##';
        }
        $fstat[$i]{'v'}{$val}++;
    }
}

sub print_log_info() {
    banner "FIELD SAMPLE VALUES";
    for my $i (@fstat) {
        printf "---- Field %03d ----\n", $i->{'name'};
        my $other = 0;
        for my $k ( sort { $i->{'v'}{$a} <=> $i->{'v'}{$b} }
            keys( %{ $i->{'v'} } ) )
        {

            # handle other values and print the in the footer
            if ( $k eq '##---other---##' ) {
                $other = $i->{'v'}{$k};
                next;
            }
            printf( "%12d : %s\n", $i->{'v'}{$k}, substr( $k, 0, 80 ) );
        }
        printf( "\n%12d : %s\n", $other, "Other values" ) if $other > 0;
        printf( "\n%12s : Min length: %8d, Max length: %8d\n\n",
            "Statistics", $i->{'minlen'}, $i->{'maxlen'} );
    }
}

sub print_log_info_short() {
    banner "FIELD STATISTICS";
    for my $i (@fstat) {
        printf "Field %03d %8d %8d\n", $i->{'name'}, $i->{'minlen'},
          $i->{'maxlen'};
    }
}

sub report_detailed_eps() {
    for my $k ( sort keys %ts_eps ) {
        printf "%s %6d %6d\n", $k, $ts_eps{$k}, $ts_data{$k};
    }
}

sub report_eps() {
    my %hour_eps  = ();
    my %hour_data = ();
    my %hour_cnt  = ();

    for my $k ( keys %ts_eps ) {
        $k =~ /^\d{4}-\d{2}-\d{2} \d{2}:/;
        $hour_cnt{$&}++;
        $hour_eps{$&}  += $ts_eps{$k};
        $hour_data{$&} += $ts_data{$k};
    }

    banner "EPS and DATA/sec REPORT";
    printf "%16s %16s %16s %16s %16s\n", " ", "Avg. Logged", "Avg. Logged",
      "Avg. Actual", "Avg. Actual";
    printf "%16s %16s %16s %16s %16s\n", "Timestamp", "Event/sec", "Bytes/sec",
      "Event/sec", "Bytes/sec";
    printf "%16s %16s %16s %16s %16s\n", "-" x 16, "-" x 16, "-" x 16,
      "-" x 16, "-" x 16;
    for my $k ( sort keys %hour_eps ) {
        printf "%16s %16.2f %16.2f %16.5f %16.5f\n", $k . "00",
          $hour_eps{$k} / $hour_cnt{$k},
          $hour_data{$k} / $hour_cnt{$k},
          $hour_eps{$k} / 3600,
          $hour_data{$k} / 3600;
    }
}

open( my $fh, "<", $ARGV[0] ) or die "ERROR: can not open logfile";

my $lineno  = 0;
my $entryno = 0;
while ( my $line = <$fh> ) {
    $lineno++;
    find_timestamp( $line, $lineno );
    if ( my $entry = process_log_entry($line) ) {
        $entryno++;

        #    print_log_entry( $entry, $RAWLOG );
        analyze_log_entry($entry);
    }
}

close($fh);

print_log_info();
print_log_info_short();
report_eps();
report_ts_field();

