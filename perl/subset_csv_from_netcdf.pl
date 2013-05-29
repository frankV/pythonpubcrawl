#!/usr/local/bin/perl

##################################################################################
#
# subset_csv_from_netcdf.pl
#
# Adapted by Jocelyn Mandalou, 2/19/2013 from 
# create_subset_from_nc.pl originally writen by Jacob Rettig, 5/19/2008
#
# usage:
#
# %> ./create_subset_from_nc.pl NETCDF_INFILE OUTPUT_DIRECTORY
#
# NETCDF_INFILE = The netCDF file from which to retrieve data. This data 
# 	        is put into a csv file for a single day and ship.
# OUTPUT_DIRECTORY = The directory in which to write the csv files.
#
# This script is meant to be exclusively called by the subset_csv.pl script.
#
##################################################################################

use lib "./";
# The perl-netcdf interface
use NetCDF;

# jm
use lib "/Net/samos/codes/";

# Use predefined paths
use IncludeDirs::Perl_dirs qw(:DEFAULT);



# Database interface subroutines

# require "perl_libs/perl_db_interface.pl";
# jm
require "/usr/people/mandalou/project4_SamosCSVFile/subset_csv_perl_db_interface.pl";

# Test number of inputs arguments.
$num_arg = @ARGV;
if ( $num_arg == 2 ) {
    $infile = $ARGV[0];
    $output_dir = $ARGV[1];
}else {
    print("Incorrect number of arguments: $num_arg\n");
    exit_gracefully(
# jm
	" Please use format \"> ./subset_csv_from_netcdf.pl NETCDF_INFILE OUTPUT_DIRECTORY\"");
}

create_subset_from_nc();


sub create_subset_from_nc {
    check_infile();
    getdata();
    makefile();
}


# This subroutine check if the .nc file is good.
sub check_infile {

    # Get first three values using string manipulation internal functions.
    # NOTE: THE DIVIDER BETWEEN THE CALL SIGN AND DATE MUST BE AN "_".
    $index = index( $infile, "_" );

    $ship = substr($infile, 0, $index);
    $date_for = substr( $infile,   $index + 1, 8 );
    $year     = substr( $infile,   $index + 1, 4 );
    $month    = substr( $date_for, 4,          2 );
    $index = index( $infile, "v" );
    $version = substr( $infile, $index + 1, 3 );
    $order = "1" . substr( $infile, $index + 4, 2 );

    # Redirect stdout to log file
    $logfile = "$errorlogs_dir/$year/$month/${ship}_${date_for}$version${order}_subset_update.log";
    #print "  Opening log file: $logfile\n";

    open LOGOUT, "> $logfile"
	|| exit_gracefully("Couldn't redirect stdout to $logfile");
	
	select(LOGOUT);

    # Print some info out
    print("SHIP : $ship\n");
    print("DATE FOR: $date_for\n");
    print("YEAR: $year\n");
    print("VERSION: $version\n");
    print("ORDER: $order\n");
    
    $path__root = $public_research;
	
	if ($version >= 200 && $version < 220)
	{
		$path__root = $processing_research;
	}
	
	elsif ($version >= 220 && $version < 250)
	{
		$path__root = $autoqc_research;
	}
	
	elsif($version < 300)
	{
		$path__root = $visualqc_research;
	}
    
    $ncfile  = "$path__root/$ship/$year/$infile";
    $index   = rindex( $infile, "." );
    $csvfile = "${ship}_${date_for}.csv";

    # Check that SAMOS v25X NetCDF exists in the correct location and open it if
    # it does exist; if it does not print error statement and exit gracefully.
    if ( -e $ncfile ) {
	$ncid = NetCDF::open( "$ncfile", NetCDF::READ );
    }
    else {
	print("The file $ncfile does not exist.\n");
	print ("The file $infile does not exist in the correct directory $path__root/$ship/$year/\n");
	close(LOGOUT);
	select(STDOUT);
	send_mail_error();
    }
	
	if ($ncid == -1)
	{
		print("Could not open file $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}

    # Initialize and get value for call sign from metadata in the NetCDF file.
    $shiptest = "";
    $status = NetCDF::attget( $ncid, NetCDF::GLOBAL, "ID", \$shiptest );
    $shiptest =~ s/\0+//g;
	
	if ($status == -1)
	{
		print("Could not get callsign from $file.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}

    # NOTE:
    # For some yet unknown reason there is an extra character \0 at the end of the
    # ship name in shiptest that chomp cannot get rid of.  It is not a consistent
    # problem (maybe only when merging happens).  So, just eliminating it.  --TS

    # Test for matching call sign; exit gracefully if they do not match.
    print("TEST CALL SIGN: $ship $shiptest\n\n");
    if ( $shiptest ne $ship ) {
	$status = NetCDF::close($ncid);
	print("call sign in $ncfile: $ship does NOT match call sign of metadata: $shiptest\n");
	close(LOGOUT);
	select(STDOUT);
	send_mail_error();
    }

    # Close NetCDF file.
    $status = NetCDF::close($ncid);
}

# This subroutine check if the .nc file is good.
sub getdata {

    # Open netCDF file
    $ncid = NetCDF::open( $ncfile, RD );
    print "ncid:  $ncid\n";
    print "ncfile:  $ncfile\n";
	
	if ($ncid == -1)
	{
		print("Could not open file $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}

    # Get number of records
    $rec_id  = NetCDF::dimid( $ncid, "time" );
	
	if ($rec_id == -1)
	{
		print("Could not find dimension \"time\" in $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	
    $dimname = "";
    $numrec  = -1;
    $status = NetCDF::diminq( $ncid, $rec_id, $dimname, $numrec );
	
	if ($status == -1)
	{
		print("Could not get number of records in \"time\" dimension from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	
    print "numrec:  $numrec\n";

    # Get (max) flag length
    $f_id    = NetCDF::dimid( $ncid, "f_string" );
	
	if ($f_id == -1)
	{
		print("Could not find \"f_string\" dimension in $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	
    $dimname = "";
    $flglen  = -1;
    $status = NetCDF::diminq( $ncid, $f_id, $dimname, $flglen );
	
	if ($ncid == -1)
	{
		print("Could not get number of records in \"f_string\" dimension from file $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	
    print "flglen:  $flglen\n";

    # Get flags
    $flag_id = NetCDF::varid( $ncid, "flag" );
	
	if ($flag_id == -1)
	{
		print("Could not find variable \"flag\" in $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	
    for ( $n = 0 ; $n < $numrec ; ++$n ) {
	@start = ( $n, 0 );
	@count = ( 1, $flglen );

	@fileflags = ();

	# Get flag
	$status = NetCDF::varget( $ncid, $flag_id, \@start, \@count, \@fileflags );
	
	if ($status == -1)
	{
		print("Could not get data from variable \"flag\" from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}

	$flags[$n] = [@fileflags];    # push reference to copy of @fileflags
    }
    
    $numvars = $#{$flags[0]} + 1;
    print "numvars:  $numvars\n";

    @vardata = ();
    $qcindex = "";
    $special = "";
    $missing = "";

    @missing = ("0") x ($numvars);
    @special = ("0") x ($numvars);

    @start = (0);
    @count = ($numrec);

# Loop through variables
# This is trickier than it sounds...how to determine how many vars you have?
#for ( $i = 0; $i < $numvars; $i++ )  --> does not work...will miss some vars b/c
#                                         some vars have same qcindex
#while ( NetCDF::varget( $ncid, $i, \@start, \@count, \@vardata ) == 0 )
#                                     --> does not work either...will bomb on vars
#                                         that have different dimensions
#while ( $qcindex != $numvars )       --> semiworks
# Method used:  loop until see last qcindex
# Note that if the last qcindexed variable appears in the netcdf file before some other
#    qcindexed vars, then those other vars won't be correct.  In other words, this method
#    assumes that variables are ordered by qcindex for the most part.
    $i = 0;
    while ( $qcindex != $numvars ) {
	$status = NetCDF::varget( $ncid, $i, \@start, \@count, \@vardata );
	
	if ($status == -1)
	{
		print("Could not get variable with id $i from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	
	  $status = NetCDF::attget( $ncid, $i, "qcindex",       \$qcindex );
	  
	if ($status == -1)
	{
		print("Could not get get attribute \"qcindex\" for variable with id $i from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	  
	  $status = NetCDF::varinq( $ncid, $i, $varname, $type, $ndims, \@dimids, $natts );
	  
	if ($status == -1)
	{
		print("Could not get information for variable with id $i from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
	  
	  print "var$qcindex: $varname\n";
	  if($varname ne 'time' && $varname ne 'lat' && $varname ne 'lon') {
	      $status = NetCDF::attget( $ncid, $i, "missing_value", \$missing );
		  
	if ($status == -1)
	{
		print("Could not get attribute \"missing_value\" for variable $varname from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
		  
		$status = NetCDF::attget( $ncid, $i, "special_value", \$special );
		
	if ($status == -1)
	{
		print("Could not get attribute \"special_value\" for variable $varname from $ncfile.\n");
		close(LOGOUT);
		select(STDOUT);
		send_mail_error();
	}
		
		
		$missing[ $qcindex - 1 ] = $missing;
		$special[ $qcindex - 1 ] = $special;
		
		print "missing$qcindex: $missing\n";
		print "special$qcindex: $special\n";
	    }

	  # Pidgeon-hole reference to vardata in right hole by qcindex
	  $data[ $qcindex - 1 ] = [@vardata];

	  $varnames[ $qcindex - 1 ] = $varname;
	  
	  ++$i;
      }
    
    NetCDF::varget( $ncid, $numvars, \@start, \@count, \@vardata );
    NetCDF::attget( $ncid, $i, "qcindex",       \$qcindex );
    NetCDF::varinq( $ncid, $numvars, $varname, $type, $ndims, \@dimids, $natts );
    @date_var = @vardata;
    print "var$qcindex: $varname\n";
    
    NetCDF::varget( $ncid, $numvars+1, \@start, \@count, \@vardata );
    NetCDF::attget( $ncid, $i, "qcindex",       \$qcindex );
    NetCDF::varinq( $ncid, $numvars+1, $varname, $type, $ndims, \@dimids, $natts );
    @time_var = @vardata;
    print "var$qcindex: $varname\n";

    # Close netCDF file
    NetCDF::close($ncid);
}

sub makefile {
    open CSV, "> $output_dir/$csvfile"
    	|| exit_gracefully( "Could not open $output_dir/$csvfile for writing" );
    print "CSV file: $output_dir/$csvfile\n";
    select(CSV);
    $seperator = ",";
    spacer("\"YYYYMMDD\"","  ");
    spacer("\"hhmmss\"","  ");
    $i = 0;
    for ( $i = 0; $i < $numvars - 1; ++$i ) {
	spacer("\"".$varnames[$i]."\"","  ");
	if($varnames[$i] = "time") {
	    $time_index = $i;
	}
    }
    $seperator = "";
    spacer("\"".$varnames[$i]."\"","  ");
    print "\n";
    
    for ( $i = 0; $i < $numrec; ++$i ) {
	$min = substr("$time_var[$i]",-4,2);
	if($min == 0) {
	    $seperator = ",";
	    spacer($date_var[$i],"  ");
	    spacer($time_var[$i],"  ");
	    for ( $j = 0; $j < $numvars - 1; ++$j ) {
		$flag = "";
		if($flags[$i][$j] ne "") {
		    $flag = sprintf(":%c", $flags[$i][$j]);
		}
		if($missing[$j] == $data[$j][$i] || $special[$j] == $data[$j][$i]) {
		    spacer("",$flag);
		} else {
		    spacer(sprintf("%.4f",$data[$j][$i]),$flag);
		}
	    }
	    $flag = "  ";
	    if($flags[$i][$j] ne "") {
		$flag = sprintf(":%c", $flags[$i][$j]);
	    }
	    $seperator = "";
	    spacer(sprintf("%.4f",$data[$j][$i]),$flag);
	    print "\n";
	}
    }
    select(STDOUT);
    
    close CSV;
}

sub spacer
{
    local(@invar) = @_;
    $spaces = "";
    $val = $invar[0];
    $flag = $invar[1];
    
    $varlength = length ($val);
    if ($varlength > 11)
    {
	$val = substr($val,0,11);
    }
    if ($varlength <= 11)
    {
	$numspaces = (11 - $varlength);
	$spaces = " " x $numspaces;
    }
    print("$spaces$val$flag$seperator");
}

# This subroutine handles error messages and was originally written by Tina Suen
sub exit_gracefully
{
    my ($error_str) = @_;

    print "Error:  $error_str\n";
    exit(1);

}

sub send_mail_error
{
    print ("ANALYST: " . $analyst_email . "\nDATE FOR: $date\nSHIP: $ship\nORDER: $order\nVERSION: $version\n");
    open MAIL, ("|/usr/lib/sendmail -t" || exit_gracefully("Could not send email to analyst\n"));
    print MAIL ("To: " . $analyst_email . "\n");
    print MAIL ("From: " . $from_email . "\n");
    print MAIL ("Subject: create_subset_from_nc.pl did not complete; o$order_no v$version for $ship/$date failed\n\n");
    print MAIL ("Use the log at $logfile for more info.\n\n\n");
    open LOGIN, "< $logfile"
	|| exit_gracefully("Couldn't redirect stdin to $logfile");
    my @lines = <LOGIN>;
    print MAIL ("@lines");
    
    close LOGIN;
    
    close MAIL;
    
    exit_gracefully("create_subset_from_nc.pl did not complete.\n\n");
}
