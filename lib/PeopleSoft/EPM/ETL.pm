#
# $Id: ETL.pm,v 0.1 2003/02/10 03:09:10 wbg Exp $

=head1 NAME

PeopleSoft::EPM::ETL - Procedural interface for querying and
manipulating the Informatica Repository

=head1 SYNOPSIS

 use PeopleSoft::EPM::ETL;
 my $return = ren_repository($old_name, $new_name, $dbh);
 my ( $maps, $srcs, $tgts, $lkps ) = get_mapping_structs( $app_aref, $dbh );
 recurse( $mapping, $load_seq, $MS, $SS, $TS );

=cut

  use DBI;

package PeopleSoft::EPM::ETL;
use Exporter;
use strict;
use Data::Dumper;
use vars qw(@ISA @EXPORT);
@ISA = qw(Exporter);

@EXPORT = qw(get_mapping_structs
	     recurse
	     ren_repository
	    );

=head1 DESCRIPTION

  This module provides a set of functions to query and manipulate
  the informatica mappings.

  The following functions are provided (and exported) by this module:

=cut

  # --------------------------------- ren_repository()

=over 3

=item ren_repository($old_name, $new_name, $dbh)

The ren_repository() function changes the name of an Informatica
repository.  It returns 1 on success and 0 on failure.

=back

=cut

sub ren_repository {
  my ( $old_name, $new_name, $dbh ) = @_;
  my ( $rec_count );

  my $sql_cmd = "select count(*) from OPB_REPOSIT";
  $sql_cmd .= " where REPOSIT_NAME = $old_name";
  my $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while ( ($rec_count) = $sth->fetchrow_array ) {
    if ( $rec_count != 1 ) {
      $sth->finish;
      return 0;
    }
  }
  $sth->finish;

  $sql_cmd = "select count(*) from OPB_REPOSIT_INFO";
  $sql_cmd .= " where REPOSITORY_NAME = $old_name";
  my $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while ( ($rec_count) = $sth->fetchrow_array ) {
    if ( $rec_count != 1 ) {
      $sth->finish;
      return 0;
    }
  }
  $sth->finish;

  $sql_cmd = "update OPB_REPOSIT";
  $sql_cmd .= " set REPOSIT_NAME = '$new_name'";
  $sql_cmd .= " where REPOSIT_NAME = '$old_name'";
  if ( ! ($sth = $dbh->prepare($sql_cmd)) ) { return 0; }
  $sth->execute;

  $sql_cmd = "update OPB_REPOSIT_INFO";
  $sql_cmd .= " set REPOSITORY_NAME = '$new_name'";
  $sql_cmd .= " where REPOSITORY_NAME = '$old_name'";
  if ( ! ($sth = $dbh->prepare($sql_cmd)) ) { return 0; }
  $sth->execute;
  $dbh->commit;
  return 1;
}
# ----------------------------------------------------------------------

=over 3

=item ( $maps, $srcs, $tgts, $lkps ) = get_mapping_structs( $app_aref, $dbh );

This function returns references to four seperate hashes that contain critical
dependency information regarding informatica maps.  The first (i.e. $maps) 
contains sections for: source tables, target tables, and lookups.  The other 
three are simply inversions to expedite searchs.

The function takes an array reference containing "applications" whose maps you
want to get information on and a database handle that point to the informatica
you are analyzing.

=back

=cut

sub get_mapping_structs { 
  my ( $app_aref, $dbh ) = @_;
  my ( @results, @r2, $tbl_name, %mappings, %sources, %targets, %lookups );
  my ( $mapping );
  my @apps = @{$app_aref};

  # -------------- First we push the lookups onto the mappings struct

  my $sql_cmd = 
    "select distinct opb_mapping.mapping_name, opb_widget_attr.attr_value 
     from opb_mapping, OPB_WIdget_inst, opb_widget_attr
     where ( opb_mapping.mapping_name like '";

  $sql_cmd .= join "\%' or opb_mapping.mapping_name like '", @apps;
  $sql_cmd .= 
    "\%' ) and opb_mapping.mapping_id = opb_widget_inst.mapping_id and 
      opb_widget_inst.instance_name like 'lkp\%' and 
      opb_widget_attr.attr_value not like '\%VW' and 
      opb_widget_inst.widget_id = opb_widget_attr.widget_id and 
      opb_widget_attr.attr_id = 2
      order by mapping_name";

  my $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while ( ( @results ) = $sth->fetchrow_array ) {
    push @{ $lookups{$results[1]} }, $results[0];
    push @{ $mappings{$results[0]}{LKPS} }, $results[1];
  }
  
  $sth->finish;

  # ------------- Next we push the sources onto mappings
  # ------------- and push mappings onto the sources struct

  $sql_cmd = 
    "select opb_mapping.mapping_name, opb_src.source_name
     from opb_mapping, opb_widget_inst, opb_src
     where ( opb_mapping.mapping_name like '";

  $sql_cmd .= join "\%' or opb_mapping.mapping_name like '", @apps;
  $sql_cmd .= 
    "\%' ) and opb_mapping.mapping_id = opb_widget_inst.mapping_id
     and opb_widget_inst.widget_type = 1
     and opb_src.src_id = opb_widget_inst.widget_id
     and opb_src.source_name not like '%ETL%'";

  $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while ( ( @results ) = $sth->fetchrow_array ) {
    push @{ $mappings{$results[0]}{SRC} }, $results[1];
    push @{ $sources{$results[1]} }, $results[0];
  }
  $sth->finish;

  # ------------- Next we push the targets onto mappings

  $sql_cmd = 
    "select opb_mapping.mapping_name, opb_targ.target_name
     from opb_mapping, opb_widget_inst, opb_targ
     where ( opb_mapping.mapping_name like '";

  $sql_cmd .= join "\%' or opb_mapping.mapping_name like '", @apps;
  $sql_cmd .= 
    "\%' ) and opb_mapping.mapping_id = opb_widget_inst.mapping_id
     and opb_targ.target_id = opb_widget_inst.widget_id
     and opb_targ.target_name not like '%ETL%'
     and opb_widget_inst.widget_type = 2";

  $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while ( ( @results ) = $sth->fetchrow_array ) {
    $sql_cmd = "select count\(\*\) from $results[1]";
    my $sth2 = $dbh->prepare($sql_cmd);
    $sth2->execute;
    while ( ( @r2 ) = $sth2->fetchrow_array ) {
      $mappings{$results[0]}{COUNT} = $r2[0];
    }

    push @{ $mappings{$results[0]}{TGT} }, $results[1];
    push @{ $targets{$results[1]} }, $results[0];
  }
  $sth->finish;

  foreach $mapping ( keys %mappings ) {
    my $srcname = $mappings{$mapping}{SRC}->[-1];
    if ( defined $lookups{$srcname} ) {
      $mappings{$mapping}{USED} = 1;
    }
  }

  return ( \%mappings, \%sources, \%targets, \%lookups );
}
# ---------------------------------- Recurse %depends


=over 3

=item recurse( $mapping, $load_seq, $MS, $SS, $TS );

This function populates the array reference passed as its first
parameter with an ordered list of informatica maps.  The order is 
such that lookups and sources are run before their targets.

The following snippet shows typical usage employing the 
get_mapping_structs function described above.

@{$aref} = qw( HR80 );
( $MS, $SS, $TS, $LS ) = get_mapping_structs( $aref, $dbh );
foreach $mapping ( keys(%{$MS}) ) {
  recurse( $mapping, $load_seq, $MS, $SS, $TS );
}

=back

=cut

sub recurse {
  my ( $mapping, $load_seq, $MS, $SS, $TS ) = @_;
  my ( $lkp, $new_trgt );

  if ( defined $MS->{$mapping}{DONE} ) {
    return;
  }

  if ( ! defined $MS->{$mapping}{LKPS} or ! defined $MS->{$mapping}{LKPS}[0] ) {
    push_onto_load_seq( $MS, $load_seq, $mapping );
    return;
  }

  while ( $new_trgt = pop @{ $MS->{$mapping}{LKPS} } ) {
    if ( defined $TS->{$new_trgt}[0] ) {
      my $new_map = $TS->{$new_trgt}[0];
      recurse( $new_map, $load_seq, $MS, $SS, $TS );
    }
    elsif ( ! defined $MS->{$new_trgt}{DONE} ) {
      $MS->{$new_trgt}{DONE} = 1;
      push_onto_load_seq( $MS, $load_seq, "No mapping: $new_trgt");
    }
  }
  recurse( $mapping, $load_seq, $MS, $SS, $TS );
}
#-----------------------------------
sub push_onto_load_seq {
  my ( $MS, $load_seq, $mapping ) = @_;

  push( @{$load_seq}, $mapping );
  $MS->{$mapping}{DONE} = 1;
  return;
}
