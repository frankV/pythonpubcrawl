package Perl_db;

# Perl functions that interface to the SAMOS database

# 11-20-06  TS
#  - Updated to Use IncludeDirs/Perl_dirs.pm
#  - Path variables changed accordingly.

# v002  JTR
# 2-06-08
#   - Added the variable $DateAsYYYYMMDD to the db function
#     get_vars_attributes so SAMOS can use date valid.
#   - Required db_get_vars and db_create_varfile_old
#     to have a second parameter ($DateAsYYYYMMDD)

# v003  JTR
# 5-22-08
#   - Added the variable $DateAsYYYYMMDD to the db function
#     get_vars_for_ship so SAMOS can use date valid.

use lib "..";
use IncludeDirs::Perl_dirs qw(:DEFAULT);
use Time::Local;
use POSIX;
use Data::Dumper;

#use lib "$codes_dir/perl_libs/Serialize/PHP-Serialization-0.27/lib";
#use lib "/Net/rvsmdc/samos/codes_working/perl_libs/Serialize/PHP-Serialization-0.27/lib";

#use PHP::Serialization qw(serialize unserialize);

#$php_code = "$php_dir/automated.inc.php";

require "$codes_dir/perl_libs/perl_db_wrapper.pl";

db_connect( $dbhost, $dbuser, $dbpass, $dbname );

sub db_interface
{
#   # Get serialized string from php script
#   $command = "/usr/local/bin/php -f $php_code";
#   $str = `$command @_ `;
#   chomp $str;
#
#   #print "STR: $str\n";
#
#   # Unserialize returned string and return
#   return unserialize($str);
    if (scalar(@_) >= 1)
    {
	my $func_name = shift;
	my $func_ref = *$func_name;
	my $ret_val = &$func_ref(@_);


	#functions calling with this wrapper function except 0 and 1 instead
	# of FALSE and TRUE
	if ($ret_val eq TRUE)
	{
	    return 1;
	}

	if ($ret_val eq FALSE)
	{
	    return 0;
	}

	return $ret_val;
    }
}

sub get_vars {

    if ( @_ != 2 ) {
	print("Wrong number of parameters to get_vars.\tGot ".@_." and expect 2.\n");
	return FALSE;
    }

    ($ship)           = $_[0];
    ($DateAsYYYYMMDD) = $_[1];
    
    #$scratch_dir = '/Net/rvsmdc/samos/scratch';
    #$scratch_dir = '../temp';
    
    #$command = "/usr/local/bin/php -f $php_code";
    #$str = `$command get_vars_for_ship $ship $DateAsYYYYMMDD`;
    #chomp $str;
    
    #print "str: $str\n";
    
    #$vars_ref = unserialize($str);
    $vars_ref = get_vars_for_ship( $ship, $DateAsYYYYMMDD );
    
    #print "vars:  $vars_ref\n";
    #print "vars:  @$vars_ref\n";
    
    @lines = ();
    
    foreach $var (@$vars_ref) {
	
	#$attr_str = `$command get_vars_attributes $ship $var $DateAsYYYYMMDD`;
	#chomp $attr_str;
	
	#print "str:  $attr_str\n";
	
	#$attr_ref = unserialize($attr_str);
	$attr_ref = get_vars_attributes( $ship, $var, $DateAsYYYYMMDD );
	
	$line = "$var, "
	    . "$attr_ref->{minimum_value}, "
	    . "$attr_ref->{maximum_value}, "
	    . "$attr_ref->{special_value}, "
	    . "$attr_ref->{missing_value}, "
	    . "$attr_ref->{variable_name}, "
	    . "$attr_ref->{units}, "
	    . "$attr_ref->{original_units},";
	
	push( @lines, $line );
    }
    
    return \@lines;
    
}

#sub db_create_varfile_old
#{
#   ($ship) = $_[0];
#   ($DateAsYYYYMMDD) = $_[1];
#
#   #$scratch_dir = '/Net/rvsmdc/samos/scratch';
#   #$scratch_dir = '../temp';
#   open VARFILE, "> $scratch_dir/KnownVarNames.$ship"
#       || die "Error opening $scratch_dir/KnownVarNames for writing\n";
#
#   $command = "/usr/local/bin/php -f $php_code";
#   $str = `$command get_vars_for_ship $ship `;
#   chomp $str;
#
#   #print "str: $str\n";
#
#   $vars_ref = unserialize($str);
#
#   #print "vars:  $vars_ref\n";
#   #print "vars:  @$vars_ref\n";
#
#   foreach $var (@$vars_ref)
#   {
#      $attr_str = `$command get_vars_attributes $ship $var $DateAsYYYYMMDD`;
#      chomp $attr_str;
#
#      #print "str:  $attr_str\n";
#
#     $attr_ref = unserialize($attr_str);
#
#      #print "attr:  $attr_ref\n";
#
#      print VARFILE "$var, ";
#      print VARFILE "$attr_ref->{minimum_value}, ";
#      print VARFILE "$attr_ref->{maximum_value}, ";
#      print VARFILE "$attr_ref->{special_value}, ";
#      print VARFILE "$attr_ref->{missing_value}, ";
#      print VARFILE "$attr_ref->{variable_name}, ";
#      print VARFILE "$attr_ref->{units}, ";
#      print VARFILE "$attr_ref->{original_units},";
#      print VARFILE "\n";
#
#   }
#
#   close VARFILE;
#
#}

# dB inc functions

sub get_error_desc {
    
    if ( @_ != 1 && @_ != 2 ) {
	print("Wrong number of parameters to get_error_desc.\tGot ".@_." and expect 1 or 2.\n");
	return FALSE;
    }
    
    my ( $error_no, $verbose ) = @_;
    if ( !$verbose ) {
	$verbose = TRUE;
    }
    
    $query = "SELECT error_description FROM error WHERE error_no=$error_no";
    db_query($query);
    
    $i         = 0;
    $max_depth = 30;
    $row       = db_get_row();
    if ( $verbose eq TRUE ) {
	$stack_trace = "#$error_no : $row->{error_description}\n";
	while (( my @call_details = ( caller( $i++ ) ) )
	       && ( $i < $max_depth ) )
	{
	    $stack_trace .=
		"  - $call_details[1] line $call_details[2] called function $call_details[3]\n";
	}
	return $stack_trace;
    }
    else {
	return $row->{error_description};
    }
}

sub get_ship_id {

    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_ship_id.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($ship_callsign) = @_;
    $query = "SELECT * FROM ship WHERE vessel_call_sign='$ship_callsign'";
    db_query($query);
    if ( db_isError() ) {
	return FALSE;
    }
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    
    return $row->{ship_id};
    
}

sub get_ship_callsign {

    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_ship_callsign.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($ship_id) = @_;
    $query = "SELECT * FROM ship WHERE ship_id='$ship_id'";
    db_query($query);
    if ( db_isError() ) {
	return FALSE;
    }
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    
    return $row->{vessel_call_sign};
    
}

# get the date with the most recent end_date (i.e. the largest)
sub get_current_date {

    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_current_date.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($ship_id) = @_;
    $query =
	"SELECT * FROM variable "
	. "WHERE ship_id=$ship_id AND date_valid_end=0 LIMIT 1";
    db_query($query);
    
    if ( db_isError() ) {
	return FALSE;
    }
    if ( db_num_rows() == 0 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    
    return $row->{date_valid_start};
}

sub get_variable_id {

    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_variable_id.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($our_variable_name) = @_;
    
    $query =
	"SELECT variable_id, variable_name FROM "
	. "known_variable WHERE variable_name='$our_variable_name'";
    db_query($query);
    if ( db_num_rows() == 0 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{variable_id};
}

sub get_missing_value {

    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_missing_value.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($variable_id) = @_;
    $query =
	'SELECT missing_value FROM known_variable WHERE variable_id='
	. $variable_id;
    db_query($query);
    
    if ( db_num_rows() > 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    
    return $row->{missing_value};
    
}

sub get_special_value {

    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_special_value.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($variable_id) = @_;
    $query =
	'SELECT special_value FROM known_variable WHERE variable_id='
	. $variable_id;
    db_query($query);
    
    if ( db_num_rows() > 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{special_value};
    
}

sub get_known_variables {

    if ( @_ != 0 ) {
	print("Wrong number of parameters to get_known_variables.\tGot ".@_." and expect 0.\n");
	return FALSE;
    }

    $query =
	'SELECT variable_name,variable_id FROM known_variable ORDER BY variable_name';
    db_query($query);
    while ( $row = db_get_row() ) {
	$variable->{ $row->{variable_id} } = $row->{variable_name};
    }
    return $variable;
    
}

sub get_static_latlon_values {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_static_latlon_values.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($ship_callsign) = @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    $query = 'SELECT lat, lon FROM ship WHERE ship_id=' . $ship_id;
    db_query($query);
    
    if ( db_isError() ) {
	return get_error_desc(100);
    }
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    my @latlon;
    $latlon[0] = $row->{lat};
    $latlon[1] = $row->{lon};
    return @latlon;
    
}

# dB functions now in perl

#/*-------------------------------------------------------------------------------------------
# Function:     get_variable_mapping
#
# Author:       Scott Tesoriere
#
# Purpose:      To figure out what our variable maps to for the given callsign, as well as
#               retrieving the units and original_units attributes.
#
# Parameters:   (1) Ship Callsign
#               (2) One of our variable's names
#               (3) Date wanted as YYYYMMDD, which if left out will force the
#                   function to return the most current date
#
# Return value: An array who has no key, but whose value is an array whose first entry is
#               their variable name, second entry is the units attribute, and third entry is
#               the original_units attribute.One might expect to get one result, but it's
#               possible that two or more of the ship's variables maps into one of ours.
#               For example, a ship might use YMD and HMS, which will be mapped into our TIME
#               variable.
#------------------------------------------------------------------------------------------*/

sub get_variable_mapping {

    if ( @_ != 2 && @_ != 3 ) {
	print("Wrong number of parameters to get_variable_mapping.\tGot ".@_." and expect 2 or 3.\n");
	return FALSE;
    }

    my ( $ship_callsign, $our_variable, $date_value ) = @_;
    if ( !$date_value ) {
	$date_value = 0;
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( ( $variable_id = get_variable_id($our_variable) ) eq FALSE ) {
	return get_error_desc(112);
    }
    
    if ( $date_value == 0 ) {
	$date_value = get_current_date($ship_id);
    }
    
    $query =
	"SELECT va.attribute_name,v.abbreviation,v.value "
	. "FROM variable v LEFT JOIN variable_attribute va "
	. "ON v.attribute_id=va.variable_attribute_id "
	. "WHERE v.ship_id=$ship_id AND v.variable_id=$variable_id "
	. "AND date_valid_start<=$date_value AND ($date_value<=date_valid_end OR date_valid_end=0) "
	. "AND (va.attribute_name='units' "
	. "OR va.attribute_name='original_units')";
    db_query($query);
    
    # you would normally expect only one variable to map back
    # to one of their variables this is not the case. for
    # example time, sometimes a ship's variables map into
    # one of ours. Time on one ship maps to both YMD and
    # HMS, this is why we need to get all variable for a
    # date that matches to time
    %working = ();
    while ( $row = db_get_row() ) {
	$working{ $row->{abbreviation} }{ $row->{attribute_name} } =
	    $row->{value};
    }
    
    # keys returns size
    if ( keys(%working) < 1 ) {
	return get_error_desc(129);
    }
    
    my $units;
    my $o_units;
    my $count = 0;
    @variables = ();
    while ( ( $abbrev, $vals ) = each(%working) ) {
	while ( ( $att_name, $val ) = each(%$vals) ) {
	    if ( $att_name eq 'units' ) {
		$units = $val;
	    }
	    elsif ( $att_name eq 'original_units' ) {
		$o_units = $val;
	    }
	}
	${@$variables}[$count]->[0] = $abbrev;
	${@$variables}[$count]->[1] = $units;
	${@$variables}[$count]->[2] = $o_units;
    }
    
    return $variables;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	is_ship_by_callsign
#
# Author:	Scott Tesoriere
#
# Purpose:	To determine whether or not a ship exists with the callsign.
#
# Parameters:	(1) Ship Callsign
#
# Return value:	An error message if an error has occurred, otherwise returns true if the ship
#				exists.
#------------------------------------------------------------------------------------------*/

sub is_ship_by_callsign {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to is_ship_by_callsign.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($ship) = @_;
    $query = "SELECT vessel_call_sign FROM ship WHERE vessel_call_sign='$ship'";
    
    db_query($query);
    
    if ( db_num_rows() > 0 ) {
	return TRUE;
    }
    else {
	return get_error_desc(101);
    }
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_file_piece
#
# Author:	Scott Tesoriere
#
# Purpose:	Creates an entry into the daily_file_piece table. If an entry already exists
#			for daily_file it will use the next available order number for
#			daily_file_piece, otherwise it will start with order number 1.
#
# Parameters:	(1) Ship Callsign
#				(2) Date Collected (YYYYMMDD)
#				(3) Original filename which was used in enter_original_filename
#
# Return value:	An error message if an error has occurred, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_file_piece {

    if ( @_ != 3 ) {
	print("Wrong number of parameters to enter_file_piece.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date___collected, $original_filename ) = @_;
    $default_order_number = 1;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    # get file id
    if ( ( $file_id = get_original_file_id($original_filename) ) eq FALSE ) {
	return get_error_desc(150);
    }
    
    if ( ( $version_id = get_version_id("005") ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year );
    if ( $timestamp_collected < 0 ) {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    $order_number  = -1;
    
    # this is the case where a datecollected entry (daily_file)
    # for a ship on a day doesn't exist yet
    $daily_file_id = get_daily_file_id( $ship_id, $date___collected );
    if ( $daily_file_id eq FALSE ) {
	
	# add entry into daily_file
	$query =
	    "INSERT INTO daily_file "
	    . "(ship_id, datetime_collected) "
	    . "VALUES ('$ship_id','$date___collected"
	    . "000001')";
	db_query($query);
	
	if ( db_isError() ) {
	    return get_error_desc(100);
	}
	
	# get daily_file_id to be used for insertion into daily_file_piece
	$daily_file_id = get_daily_file_id( $ship_id, $date___collected );
	if ( $daily_file_id eq FALSE ) {
	    return get_error_desc(106);
	}
	
	$query =
	    "INSERT INTO daily_file_piece "
	    . "(daily_file_id, original_file_id, current_version_id, date_received, order_no) "
	    . "VALUES ('$daily_file_id . ','$file_id','$version_id','$timestamp_now','$default_order_number')";
	db_query($query);
	if ( db_isError() ) {
	    return get_error_desc(100);
	}
	
	# and into file_piece
	
	$order_number = $default_order_number;
    }
    
    # this is the case where a daily_file entry already exists
    #  and there should be an order_no( an entry in daily_file_piece )
    else {
	$order_no = get_next_order_number( $ship_id, $date___collected );
	if ( $order_no == 0 ) {
	    return get_error_desc(108);
	}
	$query =
	    "INSERT INTO daily_file_piece "
	    . "(daily_file_id,original_file_id, current_version_id, date_received, order_no) "
	    . "VALUES ('$daily_file_id','$file_id','$version_id','$timestamp_now','$order_no')";
	db_query($query);
	if ( db_isError() ) {
	    return get_error_desc(100);
	}
	
	# and into file_piece
	# figure out current order_no from file_piece
	
	$order_number = $order_no;
    }
    
    $daily_file_piece_id =
	get_daily_file_piece_id( $ship_id, $date___collected, $order_number );
    if ( $daily_file_piece_id eq FALSE ) {
	return get_error_desc(117);
    }
    
    $zero_id = get_version_id("000");
    
    # automatically enter 000 as unused
    if ( enter_history_information( $daily_file_piece_id, $zero_id, time() ) eq
	 FALSE )
    {
	return get_error_desc(118);
    }
    
    if (
	update_history_information( $daily_file_piece_id, 2, $zero_id,
				    "not used" ) eq FALSE
	)
    {
	return get_error_desc(118);
    }
    
    if ( enter_history_information( $daily_file_piece_id, $version_id, time() )
	 eq FALSE )
    {
	return get_error_desc(118);
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_daily_file_id
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieves the daily file id for a given ship and collection date.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#
# Return value:	False if an has occurred, otherwise returns the id for the daily file.
#------------------------------------------------------------------------------------------*/

sub get_daily_file_id {

    if ( @_ != 2 ) {
	print("Wrong number of parameters to get_daily_file_id.\tGot ".@_." and expect 2.\n");
	return FALSE;
    }

    my ( $ship_id, $date__collected ) = @_;
    $query =
	"SELECT * FROM daily_file WHERE "
	. "ship_id='$ship_id' AND datetime_collected='$date__collected"
	. "000001'";
    db_query($query);
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{daily_file_id};
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_original_file_id
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieves the original file id for a given filename.
#
# Parameters:	(1) Original Filename
#
# Return value:	False if an has occurred, otherwise returns the id for the filename.
#------------------------------------------------------------------------------------------*/

sub get_original_file_id {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_original_file_id.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($original_filename) = @_;
    $query = "SELECT * FROM original_file WHERE filename='$original_filename'";
    db_query($query);
    if ( db_isError() ) {
	return FALSE;
    }
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{original_file_id};
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_verify_bool
#
# Author:	Scott Tesoriere
#
# Purpose:	Lets the database know whether or not a particular data file from a ship
#				passed preliminary inspection.
#
# Parameters:	(1) Original Filename
#		(2) Boolean value (0 = failed, 1 = passed)
#
# Return value:	False if an error has occurred, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_verify_bool {
    
    if ( @_ != 2 ) {
	print("Wrong number of parameters to enter_verify_bool.\tGot ".@_." and expect 2.\n");
	return FALSE;
    }

    my ( $original_filename, $bool ) = @_;
    
    # get file id
    if ( ( $file_id = get_original_file_id($original_filename) ) eq FALSE ) {
	return get_error_desc(150);
    }
    
    if ( $bool == 1 || $bool == 0 ) {
	$query =
	    "UPDATE original_file SET verified='$bool' "
	    . "WHERE original_file_id='$file_id'";
	db_query($query);
	
	if ( db_isError() ) {
	    return FALSE;
	}
	
	return TRUE;
    }
    else {
	return get_error_desc(109);
    }
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_original_filename
#
# Author:	Scott Tesoriere
#
# Purpose:	To insert an entry into the original_file table
#
# Parameters:	(1) The filename without the full path
#					(2) Where or who it came from
#					(3) The date it was received
#
# Return value:	An error message if an error has occurred, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_original_filename {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to enter_original_filename.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }
    
    my ( $filename, $data_provider, $date ) = @_;
    $year   = substr( $date, 0,  4 );
    $month  = substr( $date, 4,  2 );
    $day    = substr( $date, 6,  2 );
    $hour   = substr( $date, 8,  2 );
    $minute = substr( $date, 10, 2 );
    $second = substr( $date, 12, 2 );
    
    $timestamp_received =
	php_mktime( $hour, $minute, $second, $month, $day, $year );
    if ( $timestamp_received < 0 ) {
	return get_error_desc(102);
    }
    
    $timestamp = time();
    $query     =
	"INSERT INTO original_file (filename, provider, date_received) "
	. "VALUES('$filename','$data_provider','$timestamp_received')";
    
    db_query($query);
    if ( db_isError() ) {
	return get_error_desc(100);
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_version_id
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieves the version id for a given version number.
#
# Parameters:	(1) Version Number
#
# Return value:	False if an has occurred, otherwise returns the id for the given version
#						number.
#------------------------------------------------------------------------------------------*/

sub get_version_id {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_version_id.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($version_no) = @_;
    $query = "SELECT * FROM version_no WHERE process_version_no='$version_no'";
    db_query($query);
    if ( db_isError() ) {
	return FALSE;
    }
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{version_id};
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_daily_file_piece_id
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieves the daily file piece id for a given ship, collection date,
#			and order number.
#
# Parameters:	(1) Ship Callsign
#				(2) Date Collected (YYYYMMDD)
#				(3) Order Number
#
# Return value:	False if an has occurred, otherwise returns the daily file piece id.
#------------------------------------------------------------------------------------------*/

sub get_daily_file_piece_id {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to get_daily_file_piece_id.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    my ( $ship_id, $date__collected, $order_no ) = @_;
    $daily_file_id = get_daily_file_id( $ship_id, $date__collected );
    $query =
	"SELECT * FROM daily_file_piece WHERE "
	. "daily_file_id='$daily_file_id' AND order_no='$order_no'";
    db_query($query);
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{daily_file_piece_id};
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_next_order_number
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieve the next order number in sequence.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#
# Return value:	False if an has occurred, otherwise returns the order number (which is
#						greater than zero, and begins at one) on success.
#------------------------------------------------------------------------------------------*/

# this could be rewritten in terms of get_latest_order_number
sub get_next_order_number {
    
    if ( @_ != 2 ) {
	print("Wrong number of parameters to get_next_order_number.\tGot ".@_." and expect 2.\n");
	return FALSE;
    }

    my ( $ship_id, $date___collected ) = @_;
    
    # if we dont find a daily_file_id for date
    if (
	( $daily_file_id = get_daily_file_id( $ship_id, $date___collected ) ) eq
	FALSE )
    {
	return FALSE;
    }
    else {
	$query =
	    "SELECT * FROM daily_file_piece WHERE daily_file_id='$daily_file_id'";
	db_query($query);
	if ( db_num_rows() >= 1 ) {
	    $order_no = 0;
	    while ( $row = db_get_row() ) {
		$order_no =
		    ( $order_no < ( $row->{order_no} ) )
		    ? $row->{order_no}
		: $order_no;
	    }
	    
	    # make sure we get next order number
	    $order_no++;
	    return $order_no;
	}
	else {
	    return FALSE;
	}
    }
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_history_information
#
# Author:	Scott Tesoriere
#
# Purpose:	Creates a new entry in the daily_file_history table.
#
# Parameters:	(1) Daily File Piece ID
#		(2) Version ID
#		(3) Date Processed
#
# Return value:	False if an error has occurred, otherwise returns true.
#------------------------------------------------------------------------------------------*/

sub enter_history_information {

    if ( @_ != 3 ) {
	print("Wrong number of parameters to enter_history_information.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    my ( $daily_file_piece_id, $version_id, $date_processed ) = @_;
    $query =
	"INSERT INTO daily_file_history "
	. "(daily_file_piece_id, version_id, date_processed) "
	. "VALUES ('$daily_file_piece_id','$version_id','$date_processed')";
    db_query($query);
    
    if ( db_isError() ) {
	return FALSE;
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	update_history_information
#
# Author:	Scott Tesoriere
#
# Purpose:	Updates an entry in the daily_file_history table.
#
# Parameters:	(1) Daily File Piece ID
#		(2) Passed (0 = failed, 1 = passed, 2 = unused)
#		(3) Version ID
#		(4) Description
#
# Return value:	False if an error has occurred, otherwise returns true.
#------------------------------------------------------------------------------------------*/

sub update_history_information {
    
    if ( @_ != 4 ) {
	print("Wrong number of parameters to update_history_information.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    my ( $daily_file_piece_id, $verified, $version_id, $description ) = @_;
    $query =
	"UPDATE daily_file_history SET pass='$verified', "
	. "description='$description' WHERE daily_file_piece_id='$daily_file_piece_id' AND "
	. "version_id='$version_id'";
    db_query($query);
    
    if ( db_isError() ) {
	return FALSE;
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_latest_order_number
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieves the last order number used for a particular ship and day's worth of
#				data.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Original Filename
#
# Return value:	False if an has occurred, otherwise returns the order number (which is
#						greater than zero, and begins at one) on success.
#------------------------------------------------------------------------------------------*/

sub get_latest_order_number {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to get_latest_order_number.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date___collected, $original_filename ) = @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    # get file id
    if ( ( $file_id = get_original_file_id($original_filename) ) eq FALSE ) {
	return get_error_desc(150);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) <
	 0 )
    {
	return get_error_desc(102);
    }
    
    if (
	( $daily_file_id = get_daily_file_id( $ship_id, $date___collected ) ) eq
	FALSE )
    {
	return get_error_desc(106);
	
    }
    else {
	$query =
	    'SELECT order_no FROM daily_file_piece WHERE daily_file_id='
	    . $daily_file_id;
	db_query($query);
	
	if ( db_num_rows() <= 0 ) {
	    return get_error_desc(108);
	}
	
	$order_no = 0;
	while ( $row = db_get_row() ) {
	    if ( $row->{order_no} > $order_no ) {
		$order_no = $row->{order_no};
	    }
	}
	
	return $order_no;
    }
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_vars_for_ship
#
# Author:	Scott Tesoriere
#
# Purpose:	To retrieve a list of all of the ship's variables.
#
# Parameters:	(1) Ship Callsign
#				(3) Date wanted as YYYYMMDD, which if left out will force the
#		    			 function to return the most current date
#
# Return value:	An array who has no key, but whose value is one of their abbreviations in no
#						particular order.
#------------------------------------------------------------------------------------------*/

sub get_vars_for_ship {
    
    if ( @_ != 1 && @_ != 2 ) {
	print("Wrong number of parameters to get_vars_for_ship.\tGot ".@_." and expect 1 or 2.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date_value ) = @_;
    if ( !$date_value ) {
	$date_value = 0;
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( $date_value == 0 ) {
	$date_value = get_current_date($ship_id);
    }
    
    $query =
	"SELECT DISTINCT abbreviation FROM variable WHERE ship_id=$ship_id AND date_valid_start<=$date_value AND ($date_value<=date_valid_end OR date_valid_end=0)";
    db_query($query);
    
    @variables = ();
    while ( $row = db_get_row() ) {
	push( @variables, $row->{abbreviation} );
    }
    return \@variables;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_vars_attributes
#
# Author:	Scott Tesoriere
#
# Purpose:	To retrieve a list of all of the attributes for one of the ship's variables,
#			including the maximum, minimum, special, and missing values.
#
# Parameters:	(1) Ship Callsign
#				(2) The ship's variable name
#				(3) Date wanted as YYYYMMDD, which if left out will force the
#		    			 function to return the most current date
#
# Return value:	An array whose key is an atrribute name and whose value is the value of the
#				attribute.
#------------------------------------------------------------------------------------------*/

sub get_vars_attributes {
    
    if ( @_ != 2 && @_ != 3 ) {
	print("Wrong number of parameters to get_vars_attributes.\tGot ".@_." and expect 2 or 3.\n");
	return FALSE;
    }

    my ( $ship_callsign, $their_variable, $date_value ) = @_;
    if ( !$date_value ) {
	$date_value = 0;
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( $date_value == 0 ) {
	$date_value = get_current_date($ship_id);
    }
    
    $query =
	"SELECT va.attribute_name,v.abbreviation,v.value,kv.minimum_value,kv.maximum_value,kv.special_value,kv.missing_value,kv.variable_name FROM variable v LEFT JOIN variable_attribute va ON v.attribute_id=va.variable_attribute_id LEFT JOIN known_variable kv ON v.variable_id=kv.variable_id WHERE v.ship_id=$ship_id AND v.abbreviation='$their_variable' AND (va.attribute_name='units' OR va.attribute_name='original_units') AND v.date_valid_start<=$date_value AND ($date_value<=v.date_valid_end OR v.date_valid_end=0)";
    
    db_query($query);
    
    @rows = (
	     'maximum_value', 'minimum_value', 'special_value', 'missing_value',
	     'variable_name'
	     );
    
    if ( db_num_rows() == 0 ) {
	return FALSE;
    }
    
    my $ret;
    
    while ( $row = db_get_row() ) {
	foreach $r (@rows) {
	    if ( !exists $ret->{$r} ) {
		$ret->{$r} = $row->{$r};
	    }
	}
	if ( !exists $ret->{units} && $row->{attribute_name} eq "units" ) {
	    $ret->{units} = $row->{value};
	}
	elsif ( !exists $ret->{original_units}
		&& $row->{attribute_name} eq "original_units" )
	{
	    $ret->{original_units} = $row->{value};
	}
    }
    return $ret;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:     update_history_entry
#
# Author:       Scott Tesoriere
#
# Purpose:      Updates a history entry in the daily_file_history table to say whether or not
#      	        a particular version/stage/step of the process failed or succeeded.
#
# Parameters:   (1) Ship Callsign
#      	       	(2) Date Collected (YYYYMMDD)
#      	       	(3) Order Number
#      	       	(4) Version Number
#      	       	(5) Passed (0 = failed, 1 = passed, 2 = unused)
#      	       	(6) A description of what occurered
#
# Return value:	An error message if an error has occurred, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub update_history_entry {
    
    if ( @_ != 6 ) {
	print("Wrong number of parameters to update_history_entry.\tGot ".@_." and expect 6.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date___collected, $order_number, $version, $passed, $description ) = @_;
    
    if ( $passed ne "0" && $passed ne "1" && $passed ne "2" ) {
	return get_error_desc(109);
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( length($version) != 3 ) {
	return get_error_desc(119);
    }
    
    if ( ( $version_id = get_version_id($version) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    
    if (($daily_file_piece_id = get_daily_file_piece_id($ship_id, $date___collected, $order_number)) eq FALSE) {
	return get_error_desc(117);
    }
    
    if (update_history_information( $daily_file_piece_id, $passed, $version_id,$description ) eq FALSE) {
	return get_error_desc(120);
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:     update_merged_history_entry
#
# Author:       Richard Gange (added 11/2/06)
#
# Purpose:      Updates a history entry in the merged_file_history table to say whether or not 
#				a particular version/stage/step of the process failed or succeeded.
#
# Parameters:   (1) Ship Callsign
#      	        (2) Date Collected (YYYYMMDD)
#      	       	(3) Order Number
#      	       	(4) Version Number
#      	       	(5) Passed (0 = failed, 1 = passed, 2 = unused)
#      	       	(6) A description of what occurered
#
# Return value:	An error message if an error has occurred, otherwise returns true on success. 
#------------------------------------------------------------------------------------------*/

sub update_merged_history_entry {

    if ( @_ != 6 ) {
	print("Wrong number of parameters to update_merged_history_entry.\tGot ".@_." and expect 6.\n");
	return FALSE;
    }
    
    my ($ship_callsign, $date___collected, $order_number, $version, $passed, $description) = @_;
    
    if ( $passed ne "0" && $passed ne "1" && $passed ne "2" ) {
	return get_error_desc(109);
    }

    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( length($version) != 3 ) {
	return get_error_desc(119);
    }
    
    if ( ( $version_id = get_version_id($version) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();

    if (($merged_file_id = get_merged_file_id($ship_id, $date___collected, $order_number)) eq FALSE) {
	return get_error_desc(117);
    }
    
    if (update_merged_history_information($merged_file_id, $passed, $version_id, $description) eq FALSE) {
	return get_error_desc(120);
    }
    
    return TRUE;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_history_entry
#
# Author:	Scott Tesoriere
#
# Purpose:	Enters a history entry in the daily_file_history for a particular ship, day's
#				worth of data, order number and version number.
#
# Parameters:   (1) Ship Callsign
#      	        (2) Date Collected (YYYYMMDD)
#      	       	(3) Order Number
#      	       	(4) Version Number
#
# Return value:	An error message if an error has occurred, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_history_entry {
    
    if ( @_ != 4 ) {
	print("Wrong number of parameters to enter_history_entry.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date___collected, $order_number, $version_no ) = @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( length($version_no) != 3 ) {
	return get_error_desc(119);
    }
    
    if ( ( $version_id = get_version_id($version_no) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    
    if (($daily_file_piece_id = get_daily_file_piece_id($ship_id, $date___collected, $order_number)) eq FALSE)
    {
	return get_error_desc(117);
    }
    
    if ( enter_history_information( $daily_file_piece_id, $version_id, time() ) eq FALSE )
    {
	return get_error_desc(120);
    }
    
    # now update current version
    $query =
	'UPDATE daily_file_piece SET current_version_id='
	. $version_id
	. ' WHERE daily_file_piece_id='
	. $daily_file_piece_id;
    db_query($query);
    if ( db_isError() ) {
	return get_error_desc(121);
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_merged_history_entry
#
# Author:	Richard Gange (added 11/2/06)
#
# Purpose:	Enters a history entry in the merged_file_history for a particular ship, day's
#				worth of data, order number and version number.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Order Number
#					(4) Version Number
#
# Return value:	An error message if an error has occurred, otherwise returns true on success. 
#------------------------------------------------------------------------------------------*/

sub enter_merged_history_entry {
    
    if ( @_ != 4 ) {
	print("Wrong number of parameters to enter_merged_history_entry.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }
    
    my ($ship_callsign, $date___collected, $order_number, $version_no) = @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( length($version_no) != 3 ) {
	return get_error_desc(119);
    }
    
    if ( ( $version_id = get_version_id($version_no) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    
    if (($merged_file_id = get_merged_file_id($ship_id, $date___collected, $order_number)) eq FALSE)
    {
	return get_error_desc(117);
    }
    
    
    if ($version_no == 200) {
	if (enter_merged_history_information($merged_file_id, $version_id, time()) eq FALSE) {
	    return get_error_desc(120);
	}
    }
    
    # now update current version
    $query = 'UPDATE merged_file SET current_version_id=' . $version_id . ' WHERE merged_file_id=' . $merged_file_id;
    db_query($query);
    if ( db_isError() ) {
	return get_error_desc(121);
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_known_variables_data
#
# Author:	Scott Tesoriere
#
# Purpose:	To retrieve all of our variables with an ordering and a data type (char,
#				float, long, etc)
#
# Parameters:	None
#
# Return value:	An array whose key is the variable name, and whose value is another array
#		whose first entry is the data type, and second entry is the order value.
#------------------------------------------------------------------------------------------*/

sub get_known_variables_data {
    
    if ( @_ != 0 ) {
	print("Wrong number of parameters to get_known_variables_data.\tGot ".@_." and expect 0.\n");
	return FALSE;
    }

    $query = 'SELECT variable_name,data_type,order_value FROM known_variable';
    db_query($query);
    
    if ( db_num_rows() == 0 ) {
	return get_error_desc(128);
    }
    
    while ( $row = db_get_row() ) {
	$variables->{ $row->{variable_name} }[0] = $row->{data_type};
	$variables->{ $row->{variable_name} }[1] = $row->{order_value};
    }
    
    return $variables;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_global_attributes
#
# Author:	Scott Tesoriere
#
# Purpose:	To retrieve all of the global attributes stored in the database which are
#				to be put in the global_attribute section of the netCDF file.
#
# Parameters:	(1) Ship Callsign
#
# Return value:	An array whose key is the variable name, and whose value is an order value.
#------------------------------------------------------------------------------------------*/

sub get_global_attributes {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_gloable_attributes.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($ship_callsign) = @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    $query = 'SELECT * FROM global_attribute ORDER BY order_value';
    
    db_query($query);
    
    if ( db_num_rows() == 0 ) {
	return get_error_desc(122);
    }
    
    @rows = ();
    my $attributes = {};
    
    # loop over all global attributes
    while ( $row = db_get_row() ) {
	push( @rows, $row );
    }
    foreach $row (@rows) {
	
	# if we need to query the database
	if ( length( $row->{table_name} ) > 0 ) {
	    $query = 'SELECT '
		. $row->{column_name} . ' AS '
		. $row->{name}
	    . ' FROM '
		. $row->{table_name}
	    . ' WHERE ship_id='
		. $ship_id;
	    db_query($query);
	    if ( db_isError() ) {
		return get_error_desc(123);
	    }
	    $rown          = db_get_row();
	    $column_to_get = $row->{name};
	    $value         = $rown->{$column_to_get};
	    
	    # append text if needed
	    if ( length( $row->{value} ) > 0 ) {
		$value .= $row->{value};
	    }
	    $attributes->{ $row->{name} }->{datatype}    = $row->{datatype};
	    $attributes->{ $row->{name} }->{value}       = $value;
	    $attributes->{ $row->{name} }->{order_value} = $row->{order_value};
	    
	}
	else {
	    $attributes->{ $row->{name} }->{datatype}    = $row->{datatype};
	    $attributes->{ $row->{name} }->{value}       = $row->{value};
	    $attributes->{ $row->{name} }->{order_value} = $row->{order_value};
	}
    }
    
#    $attributes['data_provider'] = array('datatype'=>'char', 'value'=>$row->vessel_primary_contact_name);
    
    return $attributes;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_attributes_for_netcdf
#
# Author:	Scott Tesoriere
#
# Purpose:	To retrieve a list of attributes for a variable for a particular ship
#
# Parameters:	(1) Ship Callsign
#		(2) One of our variable's names
#		(3) Date wanted as YYYYMMDD, which if left out will force the
#		    function to return the most current date
#
# Return value:	An array whose key is the attribute name (including the missing and special
#		values) and whose value is another array, whose first entry is an order value,
#		second entry is the sctual value, and third entry is the data type.
#------------------------------------------------------------------------------------------*/

sub get_attributes_for_netcdf {
    
    if ( @_ != 2 && @_ != 3 ) {
	print("Wrong number of parameters to get_attributes_for_netcdf.\tGot ".@_." and expect 2 or 3.\n");
	return FALSE;
    }

    my ( $ship_callsign, $our_variable, $date_value ) = @_;
    if ( !$date_value ) {
	$date_value = 0;
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( ( $variable_id = get_variable_id($our_variable) ) eq FALSE ) {
	return get_error_desc(112);
    }
    
    if ( $date_value == 0 ) {
	$date_value = get_current_date($ship_id);
    }
    
    $query =
	"SELECT * FROM variable INNER JOIN variable_attribute on variable.attribute_id=variable_attribute.variable_attribute_id WHERE ship_id=$ship_id AND variable_id=$variable_id AND date_valid_start<=$date_value AND ($date_value<=date_valid_end OR date_valid_end=0) AND netcdf='yes'";
    
    db_query($query);
    
    if ( db_isError() ) {
	return get_error_desc(112);
    }
    
    if ( db_num_rows() == 0 ) {
	return get_error_desc(113);
    }
    
    my $columns;
    
    while ( $row = db_get_row() ) {
	$columns->{ $row->{attribute_name} }[0] = $row->{order_value};
	$columns->{ $row->{attribute_name} }[1] = $row->{value};
	$columns->{ $row->{attribute_name} }[2] = $row->{type};
    }
    
    # HAX, fix me
    
    my $t_val = get_missing_value($variable_id);
    if ( $t_val ne "N/A" ) {
	$columns->{missing_value}[0] = 'zzzzzzzzzzzzzzzzza';
	$columns->{missing_value}[1] = $t_val;
	$columns->{missing_value}[2] = 'float';
    }
    
    $t_val = get_special_value($variable_id);
    if ( $t_val ne "N/A" ) {
	$columns->{special_value}[0] = 'zzzzzzzzzzzzzzzzzb';
	$columns->{special_value}[1] = $t_val;
	$columns->{special_value}[2] = 'float';
    }
    
    return $columns;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_summary_info_from_file
#
# Author:	Scott Tesoriere
#
# Purpose:	Enters information into the qc_summary table about variables from a file
#				constructed by the QCsummary perl program for a particular ship, day's worth
#				of data, order number and version number.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Order Number
#					(4) Version Number
#					(5) Filename (the filename MUST include the full path)
#
# Return value:	An error message if an error has occured, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_summary_info_from_file {
    
    if ( @_ != 5 ) {
	print("Wrong number of parameters to enter_summary_info_from_file.\tGot ".@_." and expect 5.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date___collected, $order_number, $version, $filename )
	= @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( length($version) != 3 ) {
	return get_error_desc(119);
    }
    
    if ( ( $version_id = get_version_id($version) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    
    # see which history entry the item belongs to so we can enter it in from the table
    if (($daily_file_history_id = get_daily_file_history_id($ship_id, $date___collected, $order_number, $version_id)) eq FALSE) {
	return get_error_desc(124);
    }
    
    # based on the way this file is developed, one could improve the summary entering speed process probbaly by a lot
    # now we have to parse this qc file an enter it all into the database
    open QCFILE, $filename
	|| exit_gracefully("Can't open $filename (created by perl_db_interface.pl)\n");
    
    @lines = <QCFILE>;
    close QCFILE;
    
    chomp @lines;
    $num_lines = @lines;
    
    $index = -1;
    
    # grab the index of the headers, which is after the line "FILE SUMMARY", describes what's in the file
    for ( $i = 0 ; $i < $num_lines ; $i++ ) {
	if ( $lines[$i] eq "FILE SUMMARY" ) {
	    $index = $i + 1;
	    last;
	}
    }
    
    if ( $index == -1 ) {
	return get_error_desc(125);
    }
    
    # get a list of all our known variable so we can determine the id's
    $known_variables = get_known_variables();
    
    #$headers = preg_split("/\t/", rtrim($lines[$index]), -1, PREG_SPLIT_NO_EMPTY);
    @headers = split( "\t", $lines[$index] );
    chomp @headers;
    shift(@headers);
    $header_size = @headers;
    while ( ( $k, $v ) = each( %{$known_variables} ) ) {
	for ( $i = 0 ; $i < $header_size ; $i++ ) {
	    if ( $headers[$i] eq $v ) {
		$variables[$i]->{known_variable_id} = $k;
	    }
	}
    }
    
    # now loop through the rest of the file, grab the row names, and put 'em in that bigass array
    for ( $i = $index + 1 ; $i < $num_lines ; $i++ ) {
	
	#$line = preg_split("/\t/", rtrim($lines[$i]), -1, PREG_SPLIT_NO_EMPTY);
	@line = split( "\t", $lines[$i] );
	chomp @line;
	
	# pop off the flag name from the front
	$flag_name = lc( shift(@line) );
	$line_size = @line;
	
	if ( $header_size != $line_size ) {
	    return get_error_desc(126);
	}
	
	# now put 'em in that array
	for ( $j = 0 ; $j < $line_size ; $j++ ) {
	    $variables[$j]->{$flag_name} = $line[$j];
	}
    }
    
    # start a transaction
    db_t_begin();
    
    $var_size = @variables;
    
    $rollback = FALSE;
    for ( $i = 0 ; $i < $line_size ; $i++ ) {
	$query = 'INSERT INTO qc_summary (';
	$query .= join( ',', keys %{ $variables[$i] } );
	$query .= ',daily_file_history_id) VALUES("';
	while ( ( $k, $v ) = each( %{ $variables[$i] } ) ) {
	    $query .= $v . '","';
	}
	$query .= $daily_file_history_id . '")';
	db_query($query);
	if ( db_isError() ) {
	    $rollback = TRUE;
	    last;
	}
    }
    
    # end it a certain way
    if ( $rollback eq TRUE ) {
	db_t_end('ROLLBACK');
	return get_error_desc(100);
    }
    else {
	db_t_end('COMMIT');
    }
    
    return TRUE;
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_daily_file_history_id
#
# Author:	Scott Tesoriere
#
# Purpose:	Retrieves the daily file history id for a given ship, collection date,
#				order number, and version number.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Order Number
#					(4) Version Number
#
# Return value:	False if an has occurred, otherwise returns the daily file history id.
#------------------------------------------------------------------------------------------*/

sub get_daily_file_history_id {
    
    if ( @_ != 4 ) {
	print("Wrong number of parameters to get_daily_file_history_id.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    my ( $ship_id, $date__collected, $order_no, $version ) = @_;
    
    # by using left joins we work our way up the hierarchy of the database
    $query = " SELECT * FROM daily_file 
		         LEFT JOIN daily_file_piece on daily_file.daily_file_id=daily_file_piece.daily_file_id 
		         LEFT JOIN daily_file_history on daily_file_piece.daily_file_piece_id=daily_file_history.daily_file_piece_id 
		     WHERE daily_file.ship_id=$ship_id AND 
		           daily_file.datetime_collected LIKE '$date__collected%' AND 
		           daily_file_piece.order_no=$order_no AND 
		           daily_file_history.version_id=$version";
    
    db_query($query);
    
    if ( db_num_rows() != 1 ) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{daily_file_history_id};
}

#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	remove_summary_info
#
# Author:	Scott Tesoriere
#
# Purpose:	Removes information from the qc_summary table, for a particular ship, day's
#				worth of data, order number, and version number.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Order Number
#					(4) Version Number
#
# Return value:	An error message if an error has occured, otherwise returns true on
#		success.
#------------------------------------------------------------------------------------------*/

sub remove_summary_info {
    
    if ( @_ != 4 ) {
	print("Wrong number of parameters to remove_summary_info.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    my ( $ship_callsign, $date___collected, $order_number, $version ) = @_;
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if ( length($version) != 3 ) {
	return get_error_desc(119);
    }
    
    if ( ( $version_id = get_version_id($version) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    
    # see which history entry the item belongs
    # to so we can enter it in from the table
    if ( ($daily_file_history_id = get_daily_file_history_id($ship_id, $date___collected, $order_number, $version_id)) eq FALSE)
    {
	return get_error_desc(124);
    }
    
    $query =
	'DELETE FROM qc_summary WHERE daily_file_history_id='
	. $daily_file_history_id;
    
    db_query($query);
    
    if ( db_isError() ) {
	return get_error_desc(127);
    }else {
	return TRUE;
    }
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_version100_files
#
# Author:	Richard Gange (finished on 7/14/06)
#
# Purpose:	Retrieves a list of all version 100 '.nc' files for all ships on a specific
#				date. Test mode can be used to allow test-ship data through or real data
#				through. If test_mode = true then only those files for the test-ships will
#				be returned. If test_mode = false (default parameter) then only real data
#				ship files will be returned. Only returns files when a merge needs to be done.
#           This function will look to see when the last merge occured (if one has run) then
#           compare that date with file piece dates to determine if a merge should be ran.
#
# Parameters:	(1) Date Collected (YYYYMMDD)
#		(2) Test Mode (default = false)
#
# Return value:	An array of version 100 filenames for given parameters.
#------------------------------------------------------------------------------------------*/

sub get_version100_files {
    
    if ( @_ != 1 && @_ != 2 ) {
	print("Wrong number of parameters to get_version100_files.\tGot ".@_." and expect 1 or 2.\n");
	return FALSE;
    }

    my ( $date___collected, $test_mode ) = @_;
    if ( !$test_mode ) {
	$test_mode = 0;
    }
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if ( ( $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year ) ) < 0 )
    {
	return get_error_desc(102);
    }
    
    # Find all daily_file_id's for $date___collected
    $query = 'SELECT * FROM daily_file WHERE datetime_collected=' . $date___collected. '000001';
    db_query($query);
    @records = ();
    while($row = db_get_row() ) {
	push( @records, $row );
    }
    
    # flag to indicate if records were returned
    $files_exist = FALSE;
    @files = ();
    
    # foreach id found....
    foreach $daily_files (@records) {
	$files_exist = TRUE;
	# find all merged_file_id's associated with a single daily_file_id and pick out the one with the highest order_no
	$merged_file_id = merged_file_id($daily_files->{daily_file_id});
	
	if ($merged_file_id != 0) {
	    # if the last merge occured before the last file piece came in then we need to remerge
	    if (get_date_processed($merged_file_id) < get_date_received($daily_files->{daily_file_id})) {
		$time_to_merge = TRUE;
	    } else {
		$time_to_merge = FALSE;
	    }
	} else {
	    $time_to_merge = TRUE;
	}
	
	if ($time_to_merge eq TRUE) {
	    print TEST "in if 3\n";
	    $query = 'SELECT ship.vessel_call_sign, daily_file.datetime_collected, daily_file_piece.order_no '. 
		'FROM daily_file '.
		'LEFT JOIN daily_file_piece on daily_file.daily_file_id=daily_file_piece.daily_file_id '.
		'LEFT JOIN daily_file_history on daily_file_piece.daily_file_piece_id=daily_file_history.daily_file_piece_id '.
		'LEFT JOIN ship on daily_file.ship_id=ship.ship_id '.
		'WHERE daily_file.daily_file_id=' .
		$daily_files->{daily_file_id} . ' AND daily_file_history.version_id=6';
	    
	    db_query($query);
	    @pieces_to_merge = ();
	    while($row = db_get_row() ) {
		push( @pieces_to_merge, $row );
	    }
	    
	    # make the file names 

	    
	    if ($test_mode == 1) { # this section only allows the "TestShips" to go through
		foreach $row (@pieces_to_merge) {
		    if (substr($row->{vessel_call_sign}, 0, 4) ne "SHIP") {
			next;
		    } else {
			push( @files, make_file_name($row->{vessel_call_sign}, $row->{datetime_collected}, $row->{order_no}) );
		    }
		} 
	    } else { # this section allows all ships other than the "TestShips" to go through
		foreach $row (@pieces_to_merge) {
		    if (substr($row->{vessel_call_sign}, 0, 4) eq "SHIP") {
			next;
		    } else {
			push( @files, make_file_name($row->{vessel_call_sign}, $row->{datetime_collected}, $row->{order_no}) );
		    }
		}
	    }
	}
    }
    
    @none = ("none");
    if ($files_exist eq FALSE) {
	return FALSE;
    } else {
	if (@files) {
	    return \@files;
	} else {
	    return \@none; # this indicates to the calling program that there are no files that need to be merged 
	}
    }
}

# local use functions for get_version100_files()
sub merged_file_id {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to merged_file_id.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($daily_file_id) = @_;
    $query = 'SELECT merged_file_id FROM merged_file WHERE daily_file_id=' . $daily_file_id . ' ORDER BY order_no DESC';
    db_query($query);
    if (db_num_rows() > 0) {
	$first_row = db_get_row();
	return $first_row->{merged_file_id};
    } else {
	return 0;
    }
}

sub get_date_processed {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_date_processed.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($merged_file_id) = @_;
    $query = 'SELECT date_processed FROM merged_file_history WHERE merged_file_id=' . $merged_file_id . ' AND version_id=7';
    db_query($query);
    $first_row = db_get_row();
    return $first_row->{date_processed};
}

sub get_date_received {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_date_recieved.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }

    my ($daily_file_id) = @_;
    $query = 'SELECT date_received FROM daily_file_piece WHERE daily_file_id=' . $daily_file_id . ' ORDER BY order_no DESC';
    db_query($query);
    $first_row = db_get_row();
    return $first_row->{date_received};
}

sub make_file_name {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to make_file_name.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    my ($ship_callsign, $date__collected, $order_no) = @_;
    $file_name = $ship_callsign;
    $file_name .= "_";
    
    # parse date
    $date = substr($date__collected, 0, 8);
    
    $file_name .= $date;

    $file_name .= "v100";
    
    if ($order_no < 10) {
	$file_name .= '0';
    }
    $file_name .= $order_no;
    
    $file_name .= ".nc";
    return $file_name;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_latest_merged_order_number
#
# Author:	Richard Gange (Spring '06)
#
# Purpose:	Retrieves the next available order number for the merged sequence. (101 - 199)
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#
# Return value:	An error message if an error has occurred, otherwise returns the merged
#						order number (which is greater than 100, and begins with 101) on success.
#------------------------------------------------------------------------------------------*/

sub get_latest_merged_order_number {

    if ( @_ != 2 ) {
	print("Wrong number of parameters to get_latest_merged_order_number.\tGot ".@_." and expect 2.\n");
	return FALSE;
    }

    my ($vessel_call_sign, $date__collected) = @_;
    
    # get ship id
    if (($ship_id = get_ship_id($vessel_call_sign)) eq FALSE) {
	return get_error_desc(151);
    }
    
    # parse date
    $date__collected = substr($date__collected, 0, 8);
    $year = substr($date__collected, 0, 4);
    $month = substr($date__collected, 4, 2);
    $day = substr($date__collected, 6, 2);
    if (($timestamp = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }
    
    $query = 'SELECT merged_file.order_no '.
	'FROM daily_file '.
	'LEFT JOIN merged_file on daily_file.daily_file_id=merged_file.daily_file_id '.
	'WHERE daily_file.datetime_collected=' . $date__collected . '000001 AND daily_file.ship_id=' . $ship_id;
    
    db_query($query);
    
    $highest_order_no = 0;
    
    while ($row = db_get_row()) {
	if (($row->{order_no} > $highest_order_no) && ($row->{order_no} < 200)) {
	    $highest_order_no = $row->{order_no};
	}
    }

    if ($highest_order_no > 100) {
	return ($highest_order_no + 1);
    } else {
	return 101;
    }
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_variable_names_ordered
#
# Author:	Scott Tesoriere
#
# Purpose:	To retrieve all of our variables and ordering for the variables.
#
# Parameters:	None
#
# Return value:	An array whose key is the variable name, and whose value is an order value.
#------------------------------------------------------------------------------------------*/

sub get_variable_names_ordered {

    if ( @_ != 0 ) {
	print("Wrong number of parameters to get_variable_names_ordered.\tGot ".@_." and expect 0.\n");
	return FALSE;
    }
    
    $query = 'SELECT variable_name,data_type,order_value FROM known_variable';
    db_query($query);

    if (db_num_rows() == 0) {
	return get_error_desc(128);
    }
    
    my $variables = {};
    while ($row = db_get_row()) {
	$variables->{$row->{variable_name}} = $row->{order_value};
    }
    
    return $variables;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_merged_file
#
# Author:	Richard Gange (added on 5/16/06)
#
# Purpose:	Creates an entry into the merged_file table.
#
# Parameters:	(1) Ship Callsign
#		(2) Date Collected (YYYYMMDD)
#
# Return value:	An error message if an error has occurred, otherwise returns true on success. 
#------------------------------------------------------------------------------------------*/

sub enter_merged_file {

    if ( @_ != 2 && @_ != 3 ) {
	print("Wrong number of parameters to enter_merged_file.\tGot ".@_." and expect 2 or 3.\n");
	return FALSE;
    }
    
    my ($ship_callsign, $date___collected, $version_no) = @_;
    if ( !$version_no ) {
	$version_no = '200';
    }
    
    # get ship id
    if (($ship_id = get_ship_id($ship_callsign)) eq FALSE) {
	return get_error_desc(151);
    }
    
    if (($current_version_id = get_version_id($version_no)) eq FALSE) {
	return get_error_desc(104);
    }

    # parse date__collected
    $date___collected = substr($date___collected, 0, 8);
    $year = substr($date___collected, 0, 4);
    $month = substr($date___collected, 4, 2);
    $day = substr($date___collected, 6, 2);
    
    if (($timestamp_collected = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }

    if (($daily_file_id = get_daily_file_id($ship_id, $date___collected)) eq FALSE) {
	return get_error_desc(106);
    }
    
    if (($order_no = get_latest_merged_order_number($ship_callsign, $date___collected)) == 0) {
	return get_error_desc(108);
    }
    
    if ($version_no != "200") {
	$order_no = $order_no -1;
	$query = 'UPDATE merged_file SET current_version_id=' . $current_version_id . ' WHERE daily_file_id=' . $daily_file_id . ' AND order_no=' . $order_no;
    } else {
	$query = 'INSERT INTO merged_file (daily_file_id, current_version_id, order_no) VALUES("' . $daily_file_id . '","' . $current_version_id . '","' . $order_no . '")';
    }
    
    db_query($query);
    if (db_isError()) {
	return get_error_desc(100);
    }
    
    if (($merged_file_id = get_merged_file_id($ship_id, $date___collected, $order_no)) eq FALSE) {
	return get_error_desc(117);
    }
    
    if (enter_merged_history_information($merged_file_id, $current_version_id, time()) eq FALSE) {
	return get_error_desc(118);
    }

    if (update_merged_history_information($merged_file_id, 1, $current_version_id, "passed") eq FALSE) {
	return get_error_desc(118);
    }
    
    return TRUE;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_merged_file_id
#
# Author:	Richard Gange (added 5/16/06)
#
# Purpose:	Retrieves the merged id for a given ship, collection date, and order number.
#
# Parameters:	(1) Ship Callsign
#		(2) Date Collected (YYYYMMDD)
#		(3) Order Number
#
# Return value:	False if an has occurred, otherwise returns the merged file id.
#------------------------------------------------------------------------------------------*/

sub get_merged_file_id {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to get_merged_file_id.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    my ($ship_id, $date__collected, $order_no) = @_;
    $daily_file_id = get_daily_file_id($ship_id, $date__collected);
    $query = 'SELECT * FROM merged_file WHERE daily_file_id=' . $daily_file_id . ' AND order_no=' . $order_no;
    db_query($query);

    if (db_num_rows() > 1) {
	return FALSE;
    } elsif (db_num_rows() == 0) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{merged_file_id};
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_merged_history_information
#
# Author:	Richard Gange (added 5/17/06)
#
# Purpose:	Creates a new entry in the merged_file_history table.
#
# Parameters:	(1) Merged File ID
#					(2) Version ID
#					(3) Date Processed
#
# Return value:	False if an error has occurred, otherwise returns true.
#------------------------------------------------------------------------------------------*/

sub enter_merged_history_information {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to enter_merged_history_information.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }
    
    my ($merged_file_id, $version_id, $date_processed) = @_;
    
    
    $query = 'REPLACE INTO merged_file_history (merged_file_id, version_id, date_processed) VALUES("' . $merged_file_id . '","' . $version_id . '","' . $date_processed . '")';
    db_query($query);
    
    if (db_isError()) {
	return FALSE;
    }
    
    return TRUE;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	update_merged_history_information
#
# Author:	Richard Gange (added 5/17/06)
#
# Purpose:	Updates an entry in the merged_history table.
#
# Parameters:	(1) Merged File ID
#		(2) Passed (0 = failed, 1 = passed, 2 = unused)
#		(3) Version ID
#		(4) Description
#
# Return value:	False if an error has occurred, otherwise returns true.
#------------------------------------------------------------------------------------------*/

sub update_merged_history_information {
    
    if ( @_ != 4 ) {
	print("Wrong number of parameters to update_merged_history_information.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }
    
    my ($merged_file_id, $verified, $version_id, $description) = @_;
    
    
    $query = 'UPDATE merged_file_history SET pass="' . $verified . '",description="' . $description . '" WHERE merged_file_id=' . $merged_file_id . ' AND version_id=' . $version_id;
    db_query($query);
    
    if (db_isError()) {
	return FALSE;
    }
    
    return TRUE;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:     get_latest_version_300
#
# Author:       Jacob Rettig (added 4/2/07)
#
# Purpose:      Retrieves the latest 3XX version for a given ship, date, and order number.
#
# Parameters:   (1) Ship Callsign
#      	        (2) Date Collected (YYYYMMDD)
#      	        (3) Order Number
#
# Return value:	False if an has occurred, otherwise returns the latest 3XX version number.
#------------------------------------------------------------------------------------------*/

sub get_latest_version_300 {
    
    if ( @_ != 3 ) {
	print("Wrong number of parameters to get_latest_version_300.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }
    
    my ($ship_callsign, $date___collected, $order_no) = @_;
    
    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    $timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year );
    if ( $timestamp_collected < 0 ) {
	return get_error_desc(102);
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }
    
    if (($daily_file_id = get_daily_file_id($ship_id, $date___collected)) eq FALSE) {
	return get_error_desc(106);
    }
    
    $query = 'SELECT max(vn.process_version_no) AS process_version_no FROM merged_file mf INNER JOIN merged_file_history mfh '.
	' ON mf.order_no='.$order_no.' AND mf.daily_file_id='.$daily_file_id.' AND mf.merged_file_id=mfh.merged_file_id '.
	' INNER JOIN version_no vn ON mfh.version_id=vn.version_id ';
    
    db_query($query);
    if (db_isError()) {
	return get_error_desc(121);
    }
    
    $version_no = -1;
    if ($row = db_get_row()) {
	$version_no = $row->{process_version_no};
    }
    
    if (300 <= $version_no && $version_no < 400) {
	return $version_no;
    } else {
	return 0;
    }
    
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:     get_original_file_from_v100
#
# Author:       Jacob Rettig (added 5/9/07)
#
# Purpose:      Retrieves the original file name for a given ship, date, and order number.
#
# Parameters:   (1) Ship Callsign
#      	        (2) Date Collected (YYYYMMDD)
#      	        (3) Order Number
#
# Return value:	False if an has occurred, otherwise returns the original file name.
#------------------------------------------------------------------------------------------*/

sub get_original_file_from_v100 {

    if ( @_ != 3 ) {
	print("Wrong number of parameters to get_original_file_from_v100.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }
    
    my ($ship_callsign, $date___collected, $order_no) = @_;

    # parse date
    $date___collected = substr( $date___collected, 0, 8 );
    $year             = substr( $date___collected, 0, 4 );
    $month            = substr( $date___collected, 4, 2 );
    $day              = substr( $date___collected, 6, 2 );
    if (($timestamp_collected = php_mktime( 0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }
    
    # get ship id
    if ( ( $ship_id = get_ship_id($ship_callsign) ) eq FALSE ) {
	return get_error_desc(151);
    }

    if (($daily_file_id = get_daily_file_id( $ship_id, $date___collected ) ) eq FALSE ) {
	return get_error_desc(106);
    }
    
    $query = 'SELECT filename FROM daily_file_piece INNER JOIN original_file USING(original_file_id) WHERE daily_file_id=' . 
	$daily_file_id . ' AND order_no=' . $order_no;
    
    db_query($query);
    if ( db_isError() ) {
	return FALSE;
    }
    
    if ( $row = db_get_row() ) {
    	return $row->{filename};
    }else {
	return FALSE;
    }
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	enter_merged_summary_info_from_file
#
# Author:	Richard Gange (added 5/17/06)
#
# Purpose:	Enters information into the merged qc_summary table about variables from a 
#				file constructed by the QCsummary perl program for a particular ship, day's 
#				worth of data, order number and version number.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Order Number
#					(4) Version Number
#					(5) Filename (the filename MUST include the full path)
#
# Return value:	An error message if an error has occured, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_merged_summary_info_from_file {
    my ($ship_callsign, $date___collected, $order_number, $version, $filename) = @_;
    # get ship id
    if (($ship_id = get_ship_id($ship_callsign)) eq FALSE) {
	return get_error_desc(151);
    }
    
    if (length($version) != 3) {
	return get_error_desc(119);
    }

    if (($version_id = get_version_id($version)) eq FALSE) {
	return get_error_desc(104);
    }

    # parse date
    $date___collected = substr($date___collected, 0, 8);
    $year = substr($date___collected, 0, 4);
    $month = substr($date___collected, 4, 2);
    $day = substr($date___collected, 6, 2);
    if (($timestamp_collected = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }
    
    $timestamp_now = time();
    
    # see which history entry the item belongs to so we can enter it in from the table
    if (($merged_file_history_id = get_merged_file_history_id($ship_id, $date___collected, $order_number, $version_id)) eq FALSE) {
	return get_error_desc(124);
    }

    # based on the way this file is developed, one could improve the summary entering speed process probbaly by a lot
    # now we have to parse this qc file an enter it all into the database
    open QCFILE, $filename
	|| exit_gracefully("Can't open $filename (created by perl_db_interface.pl)\n");
    
    @lines = <QCFILE>;
    close QCFILE;
    
    chomp @lines;
    $num_lines = @lines;
    
    $index = -1;
    
    # grab the index of the headers, which is after the line "FILE SUMMARY", describes what's in the file
    for ( $i = 0 ; $i < $num_lines ; $i++ ) {
	if ( $lines[$i] eq "FILE SUMMARY" ) {
	    $index = $i + 1;
	    last;
	}
    }
    
    if ($index == -1) {
	return get_error_desc(125);
    }
    
    # get a list of all our known variable so we can determine the id's
    $known_variables = get_known_variables();
    
    #$headers = preg_split("/\t/", rtrim($lines[$index]), -1, PREG_SPLIT_NO_EMPTY);
    @headers = split( "\t", $lines[$index] );
    chomp @headers;
    shift(@headers);
    $header_size = @headers;
    while ( ( $k, $v ) = each( %{$known_variables} ) ) {
	for ( $i = 0 ; $i < $header_size ; $i++ ) {
	    if ( $headers[$i] eq $v ) {
		$variables[$i]->{known_variable_id} = $k;
	    }
	}
    }

    # now loop through the rest of the file, grab the row names, and put 'em in that bigass array
    for ( $i = $index + 1 ; $i < $num_lines ; $i++ ) {
	
	#$line = preg_split("/\t/", rtrim($lines[$i]), -1, PREG_SPLIT_NO_EMPTY);
	@line = split( "\t", $lines[$i] );
	chomp @line;
	
	# pop off the flag name from the front
	$flag_name = lc( shift(@line) );
	$line_size = @line;
	
	if ( $header_size != $line_size ) {
	    return get_error_desc(126);
	}
	
	# now put 'em in that array
	for ( $j = 0 ; $j < $line_size ; $j++ ) {
	    $variables[$j]->{$flag_name} = $line[$j];
	}
    }
    
    # start a transaction
    db_t_begin();
    
    $var_size = @variables;
    
    $rollback = FALSE;
    for ( $i = 0 ; $i < $line_size ; $i++ ) {
	$query = 'INSERT INTO merged_qc_summary (';
	$query .= join( ',', keys %{ $variables[$i] } );
	$query .= ',merged_file_history_id) VALUES("';
	while ( ( $k, $v ) = each( %{ $variables[$i] } ) ) {
	    $query .= $v . '","';
	}
	$query .= $merged_file_history_id . '")';
	db_query($query);
	if ( db_isError() ) {
	    $rollback = TRUE;
	    last;
	}
    }
    
    # end it a certain way
    if ( $rollback eq TRUE ) {
	db_t_end('ROLLBACK');
	return get_error_desc(100);
    }
    else {
	db_t_end('COMMIT');
    }
    
    return TRUE;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_merged_file_history_id
#
# Author:	Richard Gange (added 5/17/06)
#
# Purpose:	Retrieves the merged file history id for a given ship, collection date,
#				order number, and version number.
#
# Parameters:	(1) Ship Callsign
#					(2) Date Collected (YYYYMMDD)
#					(3) Order Number
#					(4) Version Number
#
# Return value:	False if an has occurred, otherwise returns the merged file history id.
#------------------------------------------------------------------------------------------*/

sub get_merged_file_history_id {

    if ( @_ != 4 ) {
	print("Wrong number of parameters to get_merged_file_history_id.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    my ($ship_id, $date__collected, $order_no, $version) = @_;

    # by using left joins we work our way up the hierarchy of the database
    $query = " SELECT * FROM daily_file ".
	"LEFT JOIN merged_file on daily_file.daily_file_id=merged_file.daily_file_id ".
	"LEFT JOIN merged_file_history on merged_file.merged_file_id=merged_file_history.merged_file_id ".
	"WHERE daily_file.ship_id=$ship_id AND ".
	"daily_file.datetime_collected LIKE '$date__collected%' AND ".
	"merged_file.order_no=$order_no AND ".
	"merged_file_history.version_id=$version";
    
    db_query($query);

    if (db_num_rows() > 1) {
	return FALSE;
    } elsif (db_num_rows() == 0) {
	return FALSE;
    }
    
    $row = db_get_row();
    return $row->{merged_file_history_id};
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:     enter_time_avg_data_from_file
#
# Author:       Jacob Rettig (added 3/04/08)
#
# Purpose:      Enters data into the time_avg_data_history and time_avg_data tables about variables from a 
#               file constructed by the MakeTimeAvgCSV perl program for a particular ship and day.
#
# Parameters:   (1) Ship Callsign
#               (2) Date Collected (YYYYMMDD)
#               (3) Order Number
#               (4) Version
#               (5) User that ran it (optional)
#               (6) Description (optional)
#
# Return value:	An error message if an error has occured, otherwise returns true on success.
#------------------------------------------------------------------------------------------*/

sub enter_time_avg_data_from_file {
    
    if ( @_ != 4 && @_ != 5 && @_ != 6 ) {
	print("Wrong number of parameters to enter_time_avg_data_from_file.\tGot ".@_." and expect 4, 5, or 6.\n");
	return FALSE;
    }
    if(!$description) {
	$description = '';
    }
    if(!$user) {
	$user = 'unknown';
    }
    
    my ($ship_callsign, $date__collected, $order, $version, $description, $user) = @_;
    
    # get ship id
    if (($ship_id = get_ship_id($ship_callsign)) eq FALSE) {
	return get_error_desc(151);
    }

    # parse date
    $date__collected = substr($date__collected, 0, 8);
    $year = substr($date__collected, 0, 4);
    $month = substr($date__collected, 4, 2);
    $day = substr($date__collected, 6, 2);
    if (($timestamp = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }

    if (($daily_file_id = get_daily_file_id($ship_id, $date__collected)) eq FALSE) {
	return "No daily file id found in enter_time_avg_data_from_file.";
    }
    
    if ( ( $version_id = get_version_id($version) ) eq FALSE ) {
	return get_error_desc(104);
    }
    
    use POSIX qw(strftime);
    $timestamp_now = strftime "%Y%m%d%H%M%S", localtime;
	
    $filename = "${ship_callsign}_${date__collected}.csv";
    $file = "$scratch_dir/$filename";
    
    my $error = '';

    # based on the way this file is developed, one could improve the summary entering speed process probbaly by a lot
    # now we have to parse this qc file an enter it all into the database
    open CSVFILE, $file
	|| return "Cannot find file $filename in enter_time_avg_data_from_file.";
    
    if($error eq '') {
	@lines = <CSVFILE>;
	close CSVFILE;
	
	chomp @lines;
	$num_lines = @lines;
	
	
	if($num_lines <= 0) {
	    $error = "Cannot open file $filename in enter_time_avg_data_from_file.";
	}
    }
    
    if($error eq '') {
	
	# get a list of all our known variable so we can determine the id's
	$known_variables = get_known_variables();
			
	@headers = split( ',', $lines[0] );
	chomp @headers;
	$header_size = @headers;
		
	for($column_no = 0; $column_no < $header_size; ++$column_no) {
	    $headers[$column_no] =~ s/"//g;  # "
	    $headers[$column_no] =~ s/ //g;
	    if($headers[$column_no] eq 'YYYYMMDD') {
		$YYYYMMDD_key = $column_no;
	    }
	    if($headers[$column_no] eq 'hhmmss') {
		$hhmmss_key = $column_no;
	    }
	}
	print "headers: @headers\n";

	while ( ( $k, $v ) = each( %{$known_variables} ) ) {
	    for ( $i = 0 ; $i < $header_size ; $i++ ) {
		if ( $headers[$i] eq $v ) {
		    $variables{$i}->{known_variable_id} = $k;
		    last;
		}
	    }
	}
	
	# now loop through the rest of the file, grab the row names, and put'em in that bigass array
	for ($i = 1; $i < $num_lines; ++$i) {
	    @line = split( ",", $lines[$i] );
	    chomp @line;
			
	    for ( $j = 0 ; $j < $header_size ; $j++ ) {
		@vals = split( ":", $line[$j]);
		chomp @vals;
		$vals[0] =~ s/ //g;
		$vals[1] =~ s/ //g;
		#print "val $i $j: '$vals[0]'\n";
		if(($vals[1] ne '') && ($vals[0] =~ /^(-?\d+\.?\d*|-?\.\d+)$/)) {
		    $variables{$j}->{sprintf("%06d",$line[$hhmmss_key]+0)}->{val} = $vals[0];
		    $variables{$j}->{sprintf("%06d",$line[$hhmmss_key]+0)}->{flag} = $vals[1];
		}
	    }
	}
	#while (($k=>$val) = each( %variables ) ) {
	#    print "$k\n";
	#    while (($time=>$val2) = each( %{$val} ) ) {
	#	if($time eq 'known_variable_id') {
	#	    print "  k: '$time', v: '$val2'\n";
	#	}else {
	#	    print "  k: '$time', v: '$val2->{val} $val2->{flag}'\n";
	#	}
	#    }
	#}
    }
    
    
    # start a transaction
    db_t_begin();
    
    my $rollback = FALSE;
    
    if($error eq '') {
	$query = "DELETE FROM time_avg_data_history WHERE daily_file_id=$daily_file_id";
	db_query($query);
	#print "$query\n";
	if (db_isError()) {
	    $rollback = TRUE;
	    $error = "query failed: $query\n";
	}
	
	$query = "INSERT INTO time_avg_data_history (daily_file_id, user, date_processed, order_no, version_id, description, pass) VALUES ($daily_file_id,'$user',$timestamp_now,$order,$version_id,'$description', 1)";
	db_query($query);
	#print "$query\n";
	if (db_isError()) {
	    $rollback = TRUE;
	    $error .= "query failed: $query\n";
	}
	
	$query = "SELECT time_avg_data_history_id FROM time_avg_data_history WHERE daily_file_id=$daily_file_id";
	db_query($query);
	#print "$query\n";
	if (db_isError()) {
	    $rollback = TRUE;
	    $error .= "query failed: $query\n";
	}elsif(($num_rows = db_num_rows()) != 1) {
	    $rollback = TRUE;
	    $error .= "There is not the right number of ids returned. $num_rows rows.";
	}else{
	    while($row = db_get_row()) {
		$time_avg_data_history_id = $row->{time_avg_data_history_id};
	    }
	}
	
	if($rollback eq FALSE) {
	    
	    @insert_pieces = ();
	    $query = "REPLACE INTO time_avg_data (time_avg_data_history_id, known_variable_id, time, value, flag) VALUES ";
	    $p_size = 0;
	    while (($k=>$var) = each( %variables ) ) {
		$variable_id = $var->{known_variable_id};
		while (($time=>$val) = each( %{$var} ) ) {
		    if($time eq 'known_variable_id') {
			next;
		    }
		    push(@insert_pieces, "($time_avg_data_history_id,$variable_id,'$time',$val->{val},'$val->{flag}')");
		    $p_size++;
		}
	    }
	    if($p_size > 0) {
		$query .= join(", ",@insert_pieces);
		db_query($query);
		#print "$query\n";
		if (db_isError()) {
		    $rollback = TRUE;
		    $error = "query failed: $query";
		}
	    } else {
		$error = "There was nothing to enter.";
	    }
	}
    }
    
    # end it a certain way
    if ($rollback eq TRUE) {
	db_t_end('ROLLBACK');
	#return get_error_desc(100);
    } else {
	db_t_end('COMMIT');
    }

    if(($error ne '') || ($rollback eq TRUE)) {
	$query = "DELETE FROM time_avg_data_history WHERE daily_file_id=$daily_file_id";
	db_query($query);
	#print "$query\n";
	if (db_isError()) {
	    $rollback = TRUE;
	    $error = "query failed: $query";
	}
	
	$query = "INSERT INTO time_avg_data_history (daily_file_id, user, date_processed, order_no, version_id, description, pass) VALUES ($daily_file_id,'$user',$timestamp_now,$order,$version_id,'$description; $error', 0)";
	db_query($query);
	#print "$query\n";
	if (db_isError()) {
	    $rollback = TRUE;
	    $error = "query failed: $query";
	}
    }
    
    if($error eq '') {
	return TRUE;
    } else {
	return $error;
    }
}
#/*-----------------------------------------------------------------------------------------*/


#/*-------------------------------------------------------------------------------------------
# Function:	get_days_that_need_subset_update
#
# Author:	Jacob Rettig (added 3/18/08)
#
# Purpose:	Retrieves the days that need subset update for a given ship, date start, and date end.
#
# Parameters:	(1) Ship Callsign
#               (2) Date Collected Start (YYYYMMDD)
#               (3) Date Collected End (YYYYMMDD) (optional: if left out is same as date start)
#               (4) Run Failed - 1 to return failed, otherwise run only unprocessed (optional)
#
# Return value:	An array of arrays with the elements being call sign, date collected, version, and order no.
#------------------------------------------------------------------------------------------*/

sub get_days_that_need_subset_update {

    if ( @_ != 2 && @_ != 3 && @_ != 4 ) {
	print("Wrong number of parameters to get_days_that_need_subset_update.\tGot ".@_." and expect 2, 3, or 4.\n");
	return FALSE;
    }

    my ($ship_callsign, $date_start, $date_end, $run_failed) = @_;

    if ( !$date_end ) {
	$date_end = 0;
    }
    if ( !$run_failed ) {
	$run_failed = 0;
    }
    	
    if ($run_failed != 1 && "$run_failed" ne "1") {
	$run_failed = 0;
    }
    
    if ("$ship_callsign" eq "") {
	$ship_callsign = 0;
    }
    $where = '';

    # get ship id
    if ("$ship_callsign" ne "0" && !(($ship_id = get_ship_id($ship_callsign)) eq FALSE)) {
	$where .= ' AND df.ship_id=' . $ship_id;
    } elsif("$ship_callsign" ne "0") {
	return get_error_desc(151);
    }
    
    # parse date
    $year = substr($date_start, 0, 4);
    $month = substr($date_start, 4, 2);
    $day = substr($date_start, 6, 2);

    # get day collected start to have php amke time to day collected start
    $day_collected_start = timestamp_to_YYYYMMDDHHMMSS(php_mktime(0, 0, 1, $month, $day, $year));
    $year2 = substr($date_end, 0, 4);
    $month2 = substr($date_end, 4, 2);
    $day2 = substr($date_end, 6, 2);
    if ($date_end == 0 || ($day_collected_end = php_mktime(0, 0, 0, $month2, $day2 +1, $year2) - 1) <= 0) {
	$day_collected_end = php_mktime(0, 0, 0, $month, $day +1, $year) - 1;
    }
    $day_collected_end = timestamp_to_YYYYMMDDHHMMSS($day_collected_end);
    if ($day_collected_start > 0 && $day_collected_end > 0 && $date_start != 0) {
	$where .= ' AND df.datetime_collected>=' . $day_collected_start . ' AND df.datetime_collected<=' . $day_collected_end;
    }
    if("$run_failed" eq "1") {
	$where2 = "pass=0 OR ";
    }
    
#    $query = "SELECT vessel_call_sign, datetime_collected, max( mf.order_no ) AS order_no, mf.current_version_id, vn.process_version_no, th.order_no AS th_order_no, th.version_id, date_processed " .
#	"FROM time_avg_data_history th " .
#	"RIGHT JOIN daily_file df ON th.daily_file_id = df.daily_file_id " .
#	"INNER JOIN merged_file mf ON df.daily_file_id = mf.daily_file_id " .
#	"INNER JOIN ship s ON df.ship_id = s.ship_id " .
#	"INNER JOIN version_no vn ON mf.current_version_id = vn.version_id " .
#	"WHERE ($where2 th.date_processed IS NULL OR mf.order_no > th.order_no OR mf.current_version_id != th.version_id ) $where " .
#	"GROUP BY vessel_call_sign, datetime_collected";

    $query = "SELECT vessel_call_sign, datetime_collected, vn1.process_version_no AS merged_version_no, "
	. "max( mf.order_no ) AS merged_order_no, mf.current_version_id AS merged_version_id, "
	. "vn2.process_version_no AS subset_version_no, th.order_no AS subset_order_no, th.version_id AS subset_version_id, date_processed "
	. "FROM time_avg_data_history th "
	. "INNER JOIN version_no vn2 "
	. "ON th.version_id = vn2.version_id "
	. "RIGHT JOIN daily_file df "
	. "ON th.daily_file_id = df.daily_file_id "
	. "INNER JOIN merged_file mf "
	. "ON df.daily_file_id = mf.daily_file_id "
	. "INNER JOIN ship s "
	. "ON df.ship_id = s.ship_id "
	. "INNER JOIN version_no vn1 "
	. "ON mf.current_version_id = vn1.version_id "
	. "WHERE ($where2 th.date_processed IS NULL OR (mf.order_no > th.order_no) OR "
	. "(mf.order_no = th.order_no AND vn1.process_version_no > vn2.process_version_no)) $where " 
	. "GROUP BY vessel_call_sign, datetime_collected ";
	
    db_query($query);
    
    if (db_isError()) {
	return get_error_desc(121);
    }
    
    @rows = ();
    while ($row = db_get_row()) {
	my @this_row = ($row->{vessel_call_sign}, substr($row->{datetime_collected},0,8), $row->{merged_version_no}, $row->{merged_order_no});#$row->{process_version_no}, $row->{order_no});
	print "@this_row\n";
	push(@rows, \@this_row);
    }
    return \@rows;
}
#/*-------------------------------------------------------------------------------------------

#/*-------------------------------------------------------------------------------------------
# Function:	get_days_needing_merge
#
# Author:	Jacob Rettig (added 6/03/09)
#
# Purpose:	Retrieves the days that need to be merged.
#
# Parameters:	(1) Date Range End Date (YYYYMMDD)
#
# Return value:	False if an has occurred, otherwise returns the merged file history id.
#------------------------------------------------------------------------------------------*/

sub get_days_needing_merge {
    
    if ( @_ != 1 ) {
	print("Wrong number of parameters to get_days_needing_merge.\tGot ".@_." and expect 1.\n");
	return FALSE;
    }
    
    my ($date) = @_;
    
    # parse date
    $date = substr($date, 0, 8);
    $year = substr($date, 0, 4);
    $month = substr($date, 4, 2);
    $day = substr($date, 6, 2);
    if (($timestamp_collected = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }
    
    # by using left joins we work our way up the hierarchy of the database
    $query = "SELECT DISTINCT daily_file.datetime_collected ".
	"FROM daily_file ".
	"INNER JOIN daily_file_piece ON daily_file.daily_file_id = daily_file_piece.daily_file_id ".
	"AND daily_file.datetime_collected <=".$date."000001 ".
	"INNER JOIN daily_file_history ON daily_file_piece.daily_file_piece_id = daily_file_history.daily_file_piece_id ".
	"INNER JOIN ship ON ship.ship_id = daily_file.ship_id ".
	"AND daily_file.datetime_collected <=".$date."000001 ".
	"INNER JOIN version_no ON version_no.process_version_no >=100 ".
	"AND version_no.version_id = daily_file_history.version_id ".
	"LEFT JOIN merged_file ON daily_file.daily_file_id = merged_file.daily_file_id ".
	"LEFT JOIN qc_summary ON daily_file_history.daily_file_history_id = qc_summary.daily_file_history_id ".
	"WHERE merged_file.merged_file_id IS NULL";
    
    db_query($query);
    
    if (db_num_rows() == 0) {
	return FALSE;
    }
    
    @dates = ();
    while ($row = db_get_row()) {
	push(@dates, $row->{datetime_collected});
    }
    
    return \@dates;
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_latest_merged_version
#
# Author:	Jacob Rettig (added 3/18/08)
#
# Purpose:	Retrieves the latest merged version for a given ship, date, version, and order number.
#
# Parameters:	(1) Ship Callsign
#				(2) Date Collected (YYYYMMDD)
#				(3) Order Number (0 for highest)
#				(4) Version Number (0 for highest, 25X for highest 25X, 30X for highest 30X)
#
# Return value:	False if an has occurred, otherwise returns the latest merged version number.
#------------------------------------------------------------------------------------------*/

sub get_latest_merged_version {

    if ( @_ != 4 ) {
	print("Wrong number of parameters to get_latest_merged_version.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    
    my ($ship_callsign, $date__collected, $order_no, $version) = @_;
    
    # parse date
    $date__collected = substr($date__collected, 0, 8);
    $year = substr($date__collected, 0, 4);
    $month = substr($date__collected, 4, 2);
    $day = substr($date__collected, 6, 2);
    if (($timestamp_collected = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }
    
    # get ship id
    if (($ship_id = get_ship_id($ship_callsign)) eq FALSE) {
	return get_error_desc(151);
    }
    
    if (($daily_file_id = get_daily_file_id($ship_id, $date__collected)) eq FALSE) {
	return get_error_desc(106);
    }
    
    $query = "SELECT process_version_no FROM merged_file INNER JOIN version_no ON merged_file.current_version_id=version_no.version_id WHERE daily_file_id=$daily_file_id ";
    
    if($version != 0) {
	$max_v = 0;
	$min_v = 0;
	if(($version eq '25x') || ($version eq '25X')) {
	    $max_v = 259;
	    $min_v = 250;
	}elsif(($version eq '30x') || ($version eq '30X')) {
	    $max_v = 309;
	    $min_v = 300;
	}else {
	    $max_v = $min_v = $version;
	}
	
	$query .= "AND $min_v<=process_version_no AND process_version_no<=$max_v ";
    }
    
    if($order_no != 0) {
	$query .= "AND order_no=$order_no ";
    }

    $query .= "ORDER BY order_no DESC, process_version_no DESC";

    db_query($query);
    
    if (db_isError()) {
	return FALSE;
    }
    
    $version_no = -1;
    if ($row = db_get_row()) {
	$version_no = $row->{process_version_no};
    }
    
    if (200 <= $version_no && $version_no < 400) {
	return $version_no;
    } else {
	return 0;
    }
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_latest_merged_order_no
#
# Author:	Jacob Rettig (added 3/18/08)
#
# Purpose:	Retrieves the latest merged order number for a given ship, date, version, and order number.
#
# Parameters:	(1) Ship Callsign
#				(2) Date Collected (YYYYMMDD)
#				(3) Order Number (0 for highest)
#				(4) Version Number (0 for highest, 25X for highest 25X, 30X for highest 30X)
#
# Return value:	False if an has occurred, otherwise returns the latest merged version number.
#------------------------------------------------------------------------------------------*/

sub get_latest_merged_order_no {

    if ( @_ != 4 ) {
	print("Wrong number of parameters to get_latest_merged_order_no.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    
    my ($ship_callsign, $date__collected, $order_no, $version) = @_;
    
    # parse date
    $date__collected = substr($date__collected, 0, 8);
    $year = substr($date__collected, 0, 4);
    $month = substr($date__collected, 4, 2);
    $day = substr($date__collected, 6, 2);
    if (($timestamp_collected = php_mktime(0, 0, 1, $month, $day, $year)) < 0) {
	return get_error_desc(102);
    }
    
    # get ship id
    if (($ship_id = get_ship_id($ship_callsign)) eq FALSE) {
	return get_error_desc(151);
    }
    
    if (($daily_file_id = get_daily_file_id($ship_id, $date__collected)) eq FALSE) {
	return get_error_desc(106);
    }

    $query = "SELECT order_no FROM merged_file INNER JOIN version_no ON merged_file.current_version_id=version_no.version_id WHERE daily_file_id=$daily_file_id ";
    
    if($version != 0) {
	$max_v = 0;
	$min_v = 0;
	if(($version eq '25x') || ($version eq '25X')) {
	    $max_v = 259;
	    $min_v = 250;
	}elsif(($version eq '30x') || ($version eq '30X')) {
	    $max_v = 309;
	    $min_v = 300;
	}else {
	    $max_v = $min_v = $version;
	}
	
	$query .= "AND $min_v<=process_version_no AND process_version_no<=$max_v ";
    }
    
    if($order_no != 0) {
	$query .= "AND order_no=$order_no ";
    }

    $query .= "ORDER BY order_no DESC, process_version_no DESC";

    db_query($query);
    
    if (db_isError()) {
	return FALSE;
    }
    
    $order_num = -1;
    if ($row = db_get_row()) {
	$order_num = $row->{order_no};
    }
    
    if (100 <= $order_num && $order_num < 200) {
	return $order_num;
    } else {
	return 0;
    }
    
}
#/*-----------------------------------------------------------------------------------------*/

#/*-------------------------------------------------------------------------------------------
# Function:	get_file_format_and_version
#
# Author:	Geoff Montee (added 7/14/09)
#
# Purpose:	Retrieves the file format and version number for a given ship and date.
#
# Parameters:	(1) Ship Callsign
#		(2) Date Collected (YYYYMMDD)
#
# Return value:	False if an error has occurred, otherwise returns the file format and format version.
#------------------------------------------------------------------------------------------*/


sub get_file_format_and_version
{
    if (@_ != 2)
    {
	print("Wrong number of parameters to get_file_format_and_version.\tGot ".@_." and expect 2.\n");
	return FALSE;
    }
    
    my ($call_sign, $date) = @_;
    
    my $query = "SELECT file_format.file_format_name, file_format.file_format_version"
	. " FROM file_format JOIN ship"
	. " ON file_format.ship_id = ship.ship_id"
	. " WHERE ship.vessel_call_sign = '" . $call_sign . "'"
	. " AND " . $date . " >= file_format.date_valid_start"
	. " AND (" . $date . " <= file_format.date_valid_end OR file_format.date_valid_end = 0)";
    
    db_query($query);
    
    if ( db_isError() )
    {
	return FALSE;
    }
    
    if ($row = db_get_row())
    {
	return ($row->{"file_format_name"}, $row->{"file_format_version"});
    }
    
    return FALSE;
} 

#/*-----------------------------------------------------------------------------------------*/

#/*-----------------------------------------------------------
# Function:   make_sassi input
#
# Purpose:    create the SASSI input file. For a ship and 
#             the date running SASSI on, we need the file list,
#             variables list with the standard deviation and
#             minimum threshold for each variable. 
#
# Parameters: 1) SASSI input file as full path
#             2) ship callsign
#             3) date_start: 2 weeks before the date running 
#                SASSI on
#             4) date_end: the date running SASSI on
#
#  Return value: An error message if an error has occured,
#  otherwise returns true on success.
#-----------------------------------------------------------------*/


sub make_sassi_input {

    if ( @_ != 4 ) {
	print("Wrong number of parameters to make_sassi_input.\tGot ".@_." and expect 4.\n");
	return FALSE;
    }

    use List::Util qw[max];
    
    my ($SASSI_input, $ship, $date_start, $date_end) = @_;
    
    open F_SASSI_INPUT, ">$SASSI_input"
	|| return "Error opening $SASSI_input for writing.\n";
    
    $num_points_threshold = 3000;
    $data_path_v200 = $processing_research;
    $data_path_v220 = $autoqc_research;
    
    # Get the variables with consistent standard deviation and minimal threshold.
    # The results covers not only the peiod from $date_start to $date_end, but also
    # for all period. This is because we may have situation like the following:
    #    variable  stand_deviation minimal_threshold  date_valid_start  date_valid_end
    #      P           3.0           0.5                20050601          20080103
    #      P           3.0           0.5                20080104             0
    # In this case, although there are two periods, the standard deviation and minimal 
    # threshold of the variable do not change. If we only consider period from $date_start 
    # to $date_end, we will miss some files.  

    my $query = "SELECT s.vessel_call_sign, v.date_valid_start,v.date_valid_end, 
           v.value, va.attribute_name, kv.variable_name
           FROM ship s
           INNER JOIN variable v ON s.vessel_call_sign = '$ship' 
           AND s.ship_id = v.ship_id AND 
           ((v.date_valid_start <= '$date_start' AND  
	   (v.date_valid_end >=  '$date_start' OR v.date_valid_end = '0')) OR
           (v.date_valid_start <= '$date_end' AND  	
	   (v.date_valid_end >=  '$date_end' OR v.date_valid_end = '0')) OR
	   (v.date_valid_start >= '$date_start' AND 
	    v.date_valid_end <=  '$date_end' AND
            v.date_valid_end != '0')) 
           INNER JOIN variable_attribute va 
	   ON v.attribute_id=va.variable_attribute_id 
                 AND va.attribute_name IN ('standard_deviation', 'threshold')
           INNER JOIN known_variable kv 
           ON kv.variable_id = v.variable_id
           ORDER BY kv.variable_name, v.date_valid_start DESC ";
    db_query($query);

    if (db_isError()) {
	return FALSE;
    }
    
    #Get the value of standard deviation and minimal threshold 
    # for each variable at the period we concern.
    $attribute_value = {};
    while ($row = db_get_row()) {	
	$attribute_value->{$row->{variable_name}}->{$row->{date_valid_start}}->{$row->{attribute_name}}->{'value'} = $row->{value};
	$attribute_value->{$row->{variable_name}}->{$row->{date_valid_start}}->{$row->{attribute_name}}->{'date_valid_end'} = $row->{date_valid_end};
	if (($row->{date_valid_start} <= $date_end) && (($date_end <= $row->{date_valid_end}) || ($row->{date_valid_end} == 0) )) {
	    $todays_values->{$row->{variable_name}}->{$row->{attribute_name}} = $row->{value};
	}
    }

    foreach $variable_name (sort keys %{$attribute_value}) {
	$var1 = $attribute_value->{$variable_name};
	#while (($variable_name, $var1) = each( %{$attribute_value} ) ) {
	$std_deviation = -8888;
	$threshold = -8888;
	my $period_start;
	while (($date_valid_start=>$var2) = each( %{$var1} ) ) {
	    $keep = TRUE;
	    while (($attribute_name, $value_for_period) = each( %{$var2}) ) {
		if ($attribute_name eq 'standard_deviation') {
		    $std_deviation = $todays_values->{$variable_name}->{$attribute_name};
		}
		elsif ($attribute_name eq 'threshold') {
		    $threshold = $todays_values->{$variable_name}->{$attribute_name};
		}
		if ($value_for_period->{'value'} != $todays_values->{$variable_name}->{$attribute_name}) {
		    $keep = FALSE;
		}
	    }
	    if ($keep eq TRUE) {
		$period_start = max($date_start,$date_valid_start);
	    } else {
		last;
	    }
	}
	if (($std_deviation != -8888) && ($threshold != -8888)) {
	    # Check all the v200 files between $period_start and $date_end for $ship.
	    # Get the file name, variables and number of good points for each variable.
	    $query = "select s.vessel_call_sign, df.datetime_collected, mf.order_no, 
        	  vn.process_version_no, 
                  (mqcs.total + mqcs.z - mqcs.special - mqcs.missing - mqcs.b - mqcs.d) as num_points, 
                  kv.variable_name 
	          from ship s 
        	  INNER JOIN daily_file df 
	          on s.ship_id = df.ship_id and s.vessel_call_sign = '$ship'
        	  and df.datetime_collected >='${period_start}00001'
	          and df.datetime_collected <='${date_end}00001'
        	  INNER JOIN merged_file mf on df.daily_file_id = mf.daily_file_id 
	          INNER JOIN merged_file_history mfh on mf.merged_file_id = mfh.merged_file_id 
        	  INNER JOIN version_no vn on mfh.version_id = vn.version_id 
	          and (vn.process_version_no>='200' and vn.process_version_no<'250') 
        	  INNER JOIN merged_qc_summary mqcs 
	          on mfh.merged_file_history_id = mqcs.merged_file_history_id 
        	  INNER JOIN known_variable kv 
	          on mqcs.known_variable_id = kv.variable_id and 
		  kv.variable_name = '$variable_name'";
	    db_query($query);
	    
	    if (db_isError()) {
		return get_error_desc(112);
	    }
	    
	    # If the file for a same ship, same day, same version has more than 1 order.
	    # get the highest order number.
	    $temp = {};
	    
	    while ($row = db_get_row()) {
		if(!defined($temp->{$row->{datetime_collected}}->{'order_no'}) || 
		   ($temp->{$row->{datetime_collected}}->{'order_no'} < $row->{order_no})  ||
		   (($temp->{$row->{datetime_collected}}->{'order_no'} == $row->{order_no}) && 
		    ($temp->{$row->{datetime_collected}}->{'process_version_no'} < $row->{process_version_no})))
		{
		    $temp->{$row->{datetime_collected}}->{'order_no'} = $row->{order_no}; 
		    $temp->{$row->{datetime_collected}}->{'num_points'} = $row->{num_points};
		    $temp->{$row->{datetime_collected}}->{'process_version_no'} = $row->{process_version_no}; 
		}
	    }
	    
	    # For each variable, loop through the files and get the total number of the 
	    # points
	    $sum = 0;
	    while (($date,$val) = each( %{$temp} ) ) {
		$sum += $val->{'num_points'};
	    }
	    
	    if ($sum >=  $num_points_threshold) {
		$file_list = $ship."_".$date_end."_".$variable_name.".list";
		
		open F_LIST, ">$scratch_dir/$file_list"
		    || return "Error opening $scratch_dir/$file_list for writing.\n";
		
		# Get the file list, write the files to "list"
		foreach $datetime_collected (sort keys %{$temp}) {
		    $vals = $temp->{$datetime_collected};
		    #while (($datetime_collected,$vals) = each( %{$temp} ) ) {
		    $year = substr($datetime_collected,0,4);
		    $date = substr($datetime_collected,0,8);
		    $order = substr($vals->{'order_no'},1,2);
		    $version = substr($vals->{'process_version_no'},0,3);
		    # v200 files and v22x files are in different directories
		    if ($version eq '200') {
			$filename = "$data_path_v200/$ship/$year/$ship"."_".$date."v".$version.$order.".nc";
		    }
		    else {
			$filename = "$data_path_v220/$ship/$year/$ship"."_".$date."v".$version.$order.".nc";
		    }
		    print F_LIST $filename."\n";
		}
		print F_SASSI_INPUT "$variable_name $std_deviation $threshold $file_list \n";
		close F_LIST;
	    } 	
	}
    }
    
    close F_SASSI_INPUT;
    
    return TRUE;
}


sub timestamp_to_YYYYMMDDHHMMSS
{
    my ($time) = @_;
    
    my $command = "date --date \@" . int($time) . " +\%Y\%m\%d\%H\%M\%S";

    my $date = `$command`;

    return $date;
}

sub php_mktime {
    
    if ( @_ != 6 ) {
	print("Wrong number of parameters to php_mktime.\tGot ".@_." and expect 6.\n");
	return FALSE;
    }
    
    my ( $hour, $min, $sec, $month, $day, $year ) = @_;

    $hour =~ s/^0*//;
    $min =~ s/^0*//;
    $sec =~ s/^0*//;
    $month =~ s/^0*//;
    $day =~ s/^0*//;
    $year =~ s/^0*//;

    open(MKTIME, 'php -r "echo(mktime(' . int($hour) . ',' .  int($min) . ',' .  int($sec) . ',' . int($month) . ',' .  int($day) . ',' .  int($year) . '));" |')
	or die("Could not call mktime\n");

    my $time = <MKTIME> or die("Bad time read in php_mktime\n");

    chomp($time);

    return $time;
    #return mktime( $sec, $min, $hour, $day, $month - 1, $year - 1900 ) | 0;
}

#/*-------------------------------------------------------------------------------------------
# Function:	get_files_report
#
# Author:	Geoff Montee (added 1/13/12)
#
# Purpose:	Returns a report of the files processed for a day.
#
# Parameters:	(1) version
#               (2) year
#               (3) month
#               (4) day
#
# Return value:	
#------------------------------------------------------------------------------------------*/


sub get_files_report
{
    if (@_ != 4)
    {
		print("Wrong number of parameters to get_files_report.\tGot ".@_." and expect 4.\n");
		return "FALSE";
    }
	
	my $version = $_[0];
	my $year = $_[1];
	my $month = $_[2];
	my $day = $_[3];
	
	$version =~ s/^0*//;
	
	my $start_timestamp = php_mktime(0, 0, 0, $month, $day, $year);
	my $end_timestamp = php_mktime(23, 59, 59, $month, $day, $year);
	
	$version = sprintf("%03d", $version);
	
	my $query;
	
	if ($version <= 100)
	{
		$query = "SELECT s.ship_id, s.vessel_call_sign, df.datetime_collected, dfp.order_no, vn.process_version_no, dfh.description, dfh.pass "
			. "FROM ship s "
			. "JOIN daily_file df "
			. "ON s.ship_id = df.ship_id "
			. "JOIN daily_file_piece dfp "
			. "ON df.daily_file_id = dfp.daily_file_id "
			. "JOIN daily_file_history dfh "
			. "ON dfp.daily_file_piece_id = dfh.daily_file_piece_id "
			. "JOIN version_no vn "
			. "ON dfh.version_id = vn.version_id "
			. "WHERE dfh.date_processed >= " . $start_timestamp . " "
			. "AND dfh.date_processed <= " . $end_timestamp . " "
			. "AND vn.process_version_no = " . $version;
	}
	
	else
	{
		$query = "SELECT s.ship_id, s.vessel_call_sign, df.datetime_collected, mf.order_no, vn.process_version_no, mfh.description, mfh.pass "
			. "FROM ship s "
			. "JOIN daily_file df "
			. "ON s.ship_id = df.ship_id "
			. "JOIN merged_file mf "
			. "ON df.daily_file_id = mf.daily_file_id "
			. "JOIN merged_file_history mfh "
			. "ON mf.merged_file_id = mfh.merged_file_id "
			. "JOIN version_no vn "
			. "ON mfh.version_id = vn.version_id "
			. "WHERE mfh.date_processed >= " . $start_timestamp . " "
			. "AND mfh.date_processed <= " . $end_timestamp . " "
			. "AND vn.process_version_no = " . $version;
	}
	
    db_query($query);

    if (db_isError()) 
	{
		return "FALSE";
    }
	
	my @files;

    while (my $row = db_get_row())
    {	
		my $file = {};
		
		$file->{"ship_id"} = $row->{"ship_id"};
		$file->{"vessel_call_sign"} = $row->{"vessel_call_sign"};
		$file->{"year_collected"} = substr($row->{"datetime_collected"}, 0, 4);
		$file->{"month_collected"} = substr($row->{"datetime_collected"}, 4, 2);
		$file->{"day_collected"} = substr($row->{"datetime_collected"}, 6, 2);
		$file->{"order_no"} = $row->{"order_no"};
		$file->{"version_no"} = $row->{"process_version_no"};
		$file->{"description"} = $row->{"description"};
		$file->{"pass"} = $row->{"pass"};
		
		push(@files, $file);
	}
	
	return \@files;
}

#/*-------------------------------------------------------------------------------------------
# Function:	check_email_whitelist
#
# Author:	Geoff Montee (added 6/9/10)
#
# Purpose:	Checks whether an e-mail address is in the whitelist table.
#
# Parameters:	(1) E-mail address
#
# Return value:	Returns 1 if e-mail address is present in table; 0 if not; -1 if error.
#------------------------------------------------------------------------------------------*/


sub check_email_whitelist
{
    if (@_ != 1)
    {
	print("Wrong number of parameters to check_email_whitelist.\tGot ".@_." and expect 1.\n");
	return (-1);
    }
    
    my ($email) = @_;
    
    my $query = "SELECT *"
	. " FROM email_whitelist"
	. " WHERE email = '" . $email . "'";
    
    db_query($query);
    
    if ( db_isError() )
    {
	my $error = db_getError();
	print($error . "\n");
	return (-1);
    }
    
    if (db_num_rows() > 0)
    {
	return (1);
    }
    
    return (0);
} 

#/*-------------------------------------------------------------------------------------------
# Function:	check_email_blacklist
#
# Author:	Geoff Montee (added 6/9/10)
#
# Purpose:	Checks whether an e-mail address is in the blacklist table.
#
# Parameters:	(1) E-mail address
#
# Return value:	Returns 1 if e-mail address is present in table; 0 if not; -1 if error.
#------------------------------------------------------------------------------------------*/


sub check_email_blacklist
{
    if (@_ != 1)
    {
	print("Wrong number of parameters to check_email_blacklist.\tGot ".@_." and expect 1.\n");
	return (-1);
    }
    
    my ($email) = @_;
    
    my $query = "SELECT *"
	. " FROM email_blacklist"
	. " WHERE email = '" . $email . "'";
    
    db_query($query);
    
    if ( db_isError() )
    {
	my $error = db_getError();
	print($error . "\n");
	return (-1);
    }
    
    if (db_num_rows() > 0)
    {
	return (1);
    }
    
    return (0);
} 

#/*-----------------------------------------------------------------------------------------*/
# Function:	get_ndbc_files
#
# Author:	Geoff Montee (added 12/13/10)
#
# Purpose:	Returns the ndbc files defined for a given ship id.
#
# Parameters:	(1) ship_id
#
# Return value:	Returns a hash reference for the files if present in table; 0 if not; -1 if error.
#------------------------------------------------------------------------------------------*/

sub get_ndbc_files
{
    if (@_ != 1)
    {
        print("Wrong number of parameters to get_ndbc_files. Got " . @_ . " and expected 1.\n");
        return -1;
    }

    my $ship_id = $_[0];

    my $query = "SELECT * FROM ndbc_file WHERE ship_id='" . $ship_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return -1;
    }

    if (db_num_rows() <= 0)
    {
        return 0;
    }

    my $files = {};

    while (my $row = db_get_row())
    {
        $files->{$row->{'id'}} = $row->{'name'};
    }

    return $files;
}

#/*-----------------------------------------------------------------------------------------*/
# Function:	get_ndbc_variables
#
# Author:	Geoff Montee (added 12/13/10)
#
# Purpose:	Returns the ndbc files defined for a given ship id.
#
# Parameters:	(1) ship_id
#            	(2) file_id
#
# Return value:	Returns a hash reference for the variables if present in table; 0 if not; -1 if error.
#------------------------------------------------------------------------------------------*/

sub get_ndbc_variables
{
    if (@_ != 2)
    {
        print("Wrong number of parameters to get_ndbc_variables. Got " . @_ . " and expected 2.\n");
        return -1;
    }

    my $ship_id = $_[0];
    my $file_id = $_[1];

    my $query = "SELECT * FROM ndbc_file WHERE ship_id='" . $ship_id . "' AND id='" . $file_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return -1;
    }

    if (db_num_rows() <= 0)
    {
        return 0;
    }

    $query = "SELECT * FROM ndbc_variable JOIN units ON ndbc_variable.unit_id=units.id WHERE file_id='" . $file_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return -1;
    }

    if (db_num_rows() <= 0)
    {
        return 0;
    }

    my $variables = {};

    while (my $row = db_get_row())
    {
        $variables->{$row->{'variable_id'}} = {};
        $variables->{$row->{'variable_id'}}->{'abbreviation'} = $row->{'abbreviation'};
        $variables->{$row->{'variable_id'}}->{'units'} = $row->{'units'};
    }

    return $variables;
}

#/*-----------------------------------------------------------------------------------------*/
# Function:	get_ndbc_file_lock
#
# Author:	Geoff Montee (added 1/2/11)
#
# Purpose:	Returns success if the the ndbc file lock is obtained, failure is not.
#
# Parameters:	(1) file_id
#
# Return value:	Returns a 0 if the lock cannot be obtained; 1 if so.
#------------------------------------------------------------------------------------------*/

sub get_ndbc_file_lock
{
    if (@_ != 1)
    {
        print("Wrong number of parameters to get_ndbc_file_lock. Got " . @_ . " and expected 1.\n");
        return 0;
    }

    my $file_id = $_[0];

    my $query = "SELECT * FROM ndbc_file_lock WHERE file_id='" . $file_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return 0;
    }

	if (db_num_rows() == 0)
	{
		$query = "INSERT INTO ndbc_file_lock (file_id) VALUES ('" . $file_id . "')";
		
		db_query($query);
		
		if (db_isError())
		{
			print (db_getError() . "\n");
			return 0;
		}
		
		return 1;
	}
	
	return 0;
	
}

#/*-----------------------------------------------------------------------------------------*/
# Function:	release_ndbc_file_lock
#
# Author:	Geoff Montee (added 1/2/11)
#
# Purpose:	Returns success if the the ndbc file lock is released, failure is not.
#
# Parameters:	(1) file_id
#
# Return value:	Returns a 0 if the lock cannot be released; 1 if so.
#------------------------------------------------------------------------------------------*/

sub release_ndbc_file_lock
{
    if (@_ != 1)
    {
        print("Wrong number of parameters to release_ndbc_file_lock. Got " . @_ . " and expected 1.\n");
        return 0;
    }

    my $file_id = $_[0];

    my $query = "SELECT * FROM ndbc_file_lock WHERE file_id='" . $file_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return 0;
    }

	if (db_num_rows() >= 1)
	{
		my $row = db_get_row();
		
		my $id = $row->{'id'};
		
		$query = "DELETE FROM ndbc_file_lock WHERE id='" . $id . "' AND file_id='" . $file_id . "'";
		
		db_query($query);
		
		if (db_isError())
		{
			print (db_getError() . "\n");
			return 0;
		}
		
		return 1;
	}
	
	return 0;
	
}

#/*-----------------------------------------------------------------------------------------*/
# Function:	get_ndbc_file_update_time
#
# Author:	Geoff Montee (added 1/2/11)
#
# Purpose:	Returns the last update time for a given ndbc file.
#
# Parameters:	(1) file_id
#
# Return value:
#				(1) year
#				(2) month
#				(3)	day
#				(4)	hour
#				(5) min
#
#	OR:
#				(1)	-1 if error or not present
#------------------------------------------------------------------------------------------*/

sub get_ndbc_file_update_time
{
    if (@_ != 1)
    {
        print("Wrong number of parameters to get_ndbc_file_update_time. Got " . @_ . " and expected 1.\n");
        return -1;
    }

    my $file_id = $_[0];

    my $query = "SELECT * FROM ndbc_file_update WHERE file_id='" . $file_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return -1;
    }

    if (db_num_rows() <= 0)
    {
        return -1;
    }

    my $row = db_get_row();
	
	my $update_time = ();
	
	@$update_time = ($row->{'year'}, $row->{'month'}, $row->{'day'}, $row->{'hour'}, $row->{'minute'});
    
    return $update_time;
}

#/*-----------------------------------------------------------------------------------------*/
# Function:	set_ndbc_file_update_time
#
# Author:	Geoff Montee (added 1/2/11)
#
# Purpose:	Returns the last update time for a given ndbc file.
#
# Parameters:	(1) file_id
#				(2) year
#				(3) month
#				(4)	day
#				(5)	hour
#				(6) min
#
# Return value: 0 if error or 1 if success
#------------------------------------------------------------------------------------------*/

sub set_ndbc_file_update_time
{
    if (@_ != 6)
    {
        print("Wrong number of parameters to set_ndbc_file_update_time. Got " . @_ . " and expected 6.\n");
        return -1;
    }

    my $file_id = $_[0];
	my $year = $_[1];
	my $month = $_[2];
	my $day = $_[3];
	my $hour = $_[4];
	my $minute = $_[5];

    my $query = "SELECT * FROM ndbc_file_update WHERE file_id='" . $file_id . "'";

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return 0;
    }

    if (db_num_rows() >= 1)
    {
        $query = "DELETE FROM ndbc_file_update WHERE file_id='" . $file_id . "'";
		
		db_query($query);

		if (db_isError())
		{
			print (db_getError() . "\n");
			return 0;
		}
    }
	
	$query = "INSERT INTO ndbc_file_update (file_id, year, month, day, hour, minute, second) VALUES ('" 
		. $file_id . "', '" . $year . "', '" . $month . "', '" . $day . "', " . $hour . "', '" . $minute . "')"; 

    db_query($query);

    if (db_isError())
    {
        print (db_getError() . "\n");
        return 0;
    }
    
    return 1;
}


#######
#/*----------------------------------------------------------------------------------------------------------
# Function:     get_days_for_csv_file
#
# Adapted from get_days_that_need_subset_update (original author Jacob Rettig 3/18/2008)
# by Jocelyn Mandalou 2/13/2013
#
# Purpose:      Retrieves the days for a given ship, start date, and end date to eventually 
#               be made into daily csv files.
#
# Parameters:   (1) Ship Callsign (if 0, retrieve all calls signs for the given date range)
#               (2) Date Collected Start (YYYYMMDD)
#               (3) Date Collected End (YYYYMMDD)

#
# Return value: An array of arrays with the elements being call sign, date collected, version, and order no.
#------------------------------------------------------------------------------------------------------------*/
sub get_days_for_csv_file {

    # Return false if 3 parameters are not passed in.
    if ( @_ != 3 ) {
    	print("Wrong number of parameters to get_days_that_need_subset_update.\tGot ".@_." and expect 3.\n");
	return FALSE;
    }

    # Fill variables with parameters
    my ($ship_callsign, $date_start, $date_end) = @_;
    
    # Check the ship call sign parameter. Return false if the call sign is not a valid call sign or equal to zero.
    # If the call sign is valid, a where clause will be added to the database query to only retrieve data for that call sign.
    # If the call sign is 0, all call signs with valid data in the date range will be retrieved.
    $ship_id_clause = 0;
    if ("$ship_callsign" ne "0" && !(($ship_id = get_ship_id($ship_callsign)) eq FALSE)) {
	$where_ship_id_clause = ' df.ship_id=' . $ship_id;
	$ship_id_clause = 1;
    } 
    elsif("$ship_callsign" ne "0") {
	return get_error_desc(151);
    }
    
    # Parse the start date.
    $start_year = substr($date_start, 0, 4);
    $start_month = substr($date_start, 4, 2);
    $start_day = substr($date_start, 6, 2);

    # If the start date is 0 or php_mktime does not return an integer, return false.
    if ($date_start == 0 || ($day_collected_start = php_mktime(0, 0, 1, $start_month, $start_day, $start_year)) <= 0) {
    	print "Invalid start date: $date_start \n";
	return FALSE;
    }

    # Get thhe start date in the format necessary for the database query.
    $day_collected_start = timestamp_to_YYYYMMDDHHMMSS(php_mktime(0, 0, 1, $start_month, $start_day, $start_year));
    
    # Parse the end date.
    $end_year = substr($date_end, 0, 4);
    $end_month = substr($date_end, 4, 2);
    $end_day = substr($date_end, 6, 2);
    
    # If the end date is 0 or php_mktime does not return an integer, return false.
    if ($date_end == 0 || ($day_collected_end = php_mktime(0, 0, 0, $end_month, $end_day +1, $end_year) - 1) <= 0) {
    	print "Invalid end date: $date_end \n";
	return FALSE;
    }

    # Get thhe end date in the format necessary for the database query.
    $day_collected_end = timestamp_to_YYYYMMDDHHMMSS($day_collected_end);

    $where_date_clause = '';

    # Error checking
    # If either date contains a decimal, return false.
    if (($date_start =~ m/\./) || ($date_end =~ m/\./)) {
          print "Invalid date. Please use date format: YYYYMMDD (no decimal point).";
	return FALSE;
    } 
    # If either date contains fewer than 8 digits, return false.
    elsif(($date_start < 10000000)||($date_end < 10000000))
    {
          print "Invalid date. Please use date format: YYYYMMDD";
	return FALSE;
    }
    # If the start date precedes the start of the SAMOS project, return false and tell the user the SAMOS start date.
    elsif (($day_collected_start < 20050509000000) || ($date_start < 20050509))
    {
    	print("\nError: Start date " . $date_start . " predates SAMOS project. \nTo get the earliest SAMOS data, use start date 20050509.\n");
    	return FALSE;
    }
    # If the end date is earlier than the start date, return false;
    elsif(($day_collected_end < $day_collected_start) || ($date_end < $date_start)){
        	print("\nError: End date earlier than start date.\n");
	return FALSE;
    }
    # If none of the above errors occurred, set the database query WHERE clause to only retrieve dates in the date range given in the parameters.
    else{
        $where_date_clause = ' df.datetime_collected>=' . $day_collected_start . ' AND df.datetime_collected<=' . $day_collected_end;
    }

    # Beginning of the SAMOS database query.
    $query = "SELECT vessel_call_sign, datetime_collected, vn1.process_version_no AS merged_version_no, "
	. "max( mf.order_no ) AS merged_order_no, mf.current_version_id AS merged_version_id, "
	. "vn2.process_version_no AS subset_version_no, th.order_no AS subset_order_no, th.version_id AS subset_version_id, date_processed "
	. "FROM time_avg_data_history th "
	. "INNER JOIN version_no vn2 "
	. "ON th.version_id = vn2.version_id "
	. "RIGHT JOIN daily_file df "
	. "ON th.daily_file_id = df.daily_file_id "
	. "INNER JOIN merged_file mf "
	. "ON df.daily_file_id = mf.daily_file_id "
	. "INNER JOIN ship s "
	. "ON df.ship_id = s.ship_id "
	. "INNER JOIN version_no vn1 "
	. "ON mf.current_version_id = vn1.version_id "
	. " WHERE $where_date_clause ";
	
    # If a ship call sign parameter is given, add a WHERE clause to the database query to only retrieve data for that ship.	
    if($ship_id_clause == 1)
    {
	$query .= "AND" . $where_ship_id_clause;
    }
    
    # Finish the query.
    $query .= ' GROUP BY vessel_call_sign, datetime_collected ';

    db_query($query);

    # If there is an error with the database query, return with an error.
    if (db_isError()) {
	return get_error_desc(121);
    }
    
    # Prompt the user to continue. The number of files and the date range are displayed. 
    # The number of files that will be made can be very large. There are 469 files for all call signs for June, 2012, for example.
    # The user will have a chance to exit if the number of files is too large or the date range is incorrect.
    print ("\n  Number of output files: " . db_num_rows());
    # Approximate total size of output. Upper bound based on the fact that the largest output file I've seen is 10.15 KB.
    $approx_output_size = db_num_rows() * 10.15 / 1000;
    print ("\n  Approximate total output size: Up to " . $approx_output_size . " MB"); 
    print "\n  Date range $date_start to $date_end";
    print "\n  CONTINUE? 'y' to continue, anything else to exit: ";
    my $answer = <STDIN>;
    # If the user does not want to continue
    if($answer ne "y\n")
    {
    	return FALSE;
    } 
    # If the user wants to continue
    else 
    {
	@rows = ();
    	while ($row = db_get_row()) {
		my @this_row = ($row->{vessel_call_sign}, substr($row->{datetime_collected},0,8), $row->{merged_version_no}, $row->{merged_order_no});#$row->{process_version_no}, $row->{order_no});
		print "@this_row\n";
		push(@rows, \@this_row);
    	}
    	return \@rows;
    }
  
} # end get_days_for_csv_file
#/*-------------------------------------------------------------------------------------------
#######

return 1;
