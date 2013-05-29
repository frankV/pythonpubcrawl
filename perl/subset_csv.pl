#!/usr/local/bin/perl

###########################################################################################
#
# subset_csv.pl
#
# Adapted by Jocelyn Mandalou, 2/19/2013 from 
# update_subset.pl originally writen by Jacob Rettig, 5/19/2008
#
# usage:
#
# %> ./subset_csv.pl  CALL_SIGN_OR_0 START_DATE END_DATE OUTPUT_DIRECTORY
#
#     CALL_SIGN_OR_0 = call sign for a ship, or 0 for all ships
#     START_DATE = YYYYMMDD string of the requested data start date
#     END_DATE = YYYYMMDD string of the requested data end date
#     OUTPUT_DIRECTORY = directory to hold the output files (must already exist)
#
# This script, along with use of subset_csv_from_netcdf.pl and the function 
# get_days_for_csv_file in /Net/samos/codes/perl_libs/perl_db_interface.pl,  
# produces csv files of SAMOS hourly data, each file containing data for a single 
# day and ship. The get_days_for_csv_file uses the SHIP_CALL_SIGN parameter (a ship call 
# sign or 0 for all ships), the START_DATE parameter, and the END_DATE parameter 
# of this script to query the SAMOS database for a list of days with valid data.  
#
# The subset_csv_from_netcdf.pl script is run for each of the valid days to extract 
# data from netCDF files and produce a csv file. The naming format for the output 
# files is: CALLSIGN_YYYYMMDD.csv where CALLSIGN is the ship call sign and 
# YYYYMMDD is the date for the data. The csv files are output to the OUTPUT_DIRECTORY 
# parameter of this script.
# 
#
###########################################################################################

use lib "./";

# Use predefined paths
# jm
use lib "/Net/samos/codes/";

use IncludeDirs::Perl_dirs qw(:DEFAULT);
# use ../../../../Net/samos/codes/IncludeDirs::Perl_dirs qw(:DEFAULT);


# Database interface subroutines

# require "perl_libs/perl_db_interface.pl";
# jm
require "/usr/people/mandalou/project4_SamosCSVFile/subset_csv_perl_db_interface.pl";


$PERL = '/usr/local/bin/perl';

# Test number of inputs arguments.
$num_arg = @ARGV;
if ( $num_arg == 4 ) {
    $ship       = $ARGV[0];
    $date_start = $ARGV[1];
    $date_end   = $ARGV[2];
    
    if(!(-d $ARGV[3]))
    {
	exit_gracefully( "The directory " . $ARGV[3] . " does not exist. Please give a valid directory for csv file output.");
    }
    else
    {
	$output_dir = $ARGV[3];
    }
}
else {
    print("\nIncorrect number of arguments: $num_arg\n");
    exit_gracefully("Please use format \"> ./subset_csv.pl CALL_SIGN_OR_0 START_DATE END_DATE OUTPUT_DIRECTORY\"");
}

start();
do_it();

sub start {

    # Get days with data.
    print("\nGetting day list from database.\n");
    $rows = Perl_db::get_days_for_csv_file($ship, $date_start, $date_end);
    if($rows eq FALSE)
    {
    	exit_gracefully("Database retrieval cancellation due to error or user request.");
    }
    if ( ref($rows) ne 'ARRAY' ) {
	exit_gracefully("\"$rows\"");
    }elsif(@$rows <= 0) {
	print("  List is empty.  Nothing to do.\n");
    }else {
	print("  Got list from dB.\n\n");
    }
}

sub do_it {

    foreach $row (@$rows) {
	$call_sign = @$row[0];
	$date      = @$row[1];
	$version   = @$row[2];
	$order     = substr( @$row[3], 1, 2 );
	print("  Processing $call_sign on $date...\n");
	$infile  = "${call_sign}_${date}v${version}${order}.nc";
	# $command = "$PERL -I" . $codes_dir . " " .  $codes_dir . "/create_subset_from_nc.pl $infile ";
	# jm
	# $command = "$PERL -I" . $codes_dir . " " .  $codes_dir . "/subset_csv_from_netcdf.pl $infile ";	
	$myDir = " /usr/people/mandalou/project4_SamosCSVFile";
	#$command = "$PERL -I" . $myDir . " " .  $myDir . "/subset_csv_from_netcdf.pl $infile ";	
	# jm
	#$command = "perl -I" . $myDir . " " .  $myDir . "/subset_csv_from_netcdf.pl $infile ";
	$command = "perl -I" . $myDir . " " .  $myDir . "/subset_csv_from_netcdf.pl $infile $output_dir ";	
	#print "  Command: > $command\n";
	system($command ) == 0
	    || print("  Processing failed.\n");
	print("  Processing for $call_sign on $date done.\n\n");
    }
}

# This subroutine handles error messages and was originally written by Tina Suen
sub exit_gracefully {
    ($error_str) = @_;

    print "\nScript exited. Error: $error_str\n\n";
    exit(1);

}
