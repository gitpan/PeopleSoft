#
# $Id: Tables.pm,v 0.1 2003/02/10 03:09:10 wbg Exp $

=head1 NAME

PeopleSoft::DataLoader - Functions for Data Loader


=head1 SYNOPSIS

 use PeopleSoft::EPM::DataLoader;
 my $result = remove_grp($grpid,$dbh);
 my $result = create_grp(\@grp,,$grpid,$parallelflag $dbh)
 my $result = release_recsuite($rs_id, $js_id, $dbh);

=cut

package PeopleSoft::EPM::DataLoader;
use DBI;
use strict;
use Data::Dumper;
use Exporter;
use vars qw(@ISA @EXPORT);
@ISA = qw(Exporter);

@EXPORT = qw(remove_grp
             create_grp
	     release_recsuite
	     dl_run_seq
	     get_maps_aref
            );

=head1 DESCRIPTION

This module provides functionality associated with running
the data loader utility to move data from ODS staging into
the enterprise warehouse.

=cut

# --------------------------------- remove_grp()

=over 3

=item remove_grp($grpid, $dbh)

The remove_grp() deletes the specified data loader map
group from the database associated with $dbh.

=back

=cut

sub remove_grp{
   my ($grpid,$dbh) = @_;
   my @sql_cmd;
   $sql_cmd[0] = "delete from ps_pf_dl_grp_defn where pf_dl_grp_id = '$grpid'";
   $sql_cmd[1] = "delete from ps_pf_dl_grp_step where pf_dl_grp_id = '$grpid'";
   foreach (@sql_cmd){
      $dbh->do($_);
      $dbh->commit;
   }
}
# --------------------------------- create_grp()

=over 3

=item create_grp(\@grp,,$grpid,$parallelflag $dbh)

create_grp creates a data loader map group including all the
maps in $maps_aref with description of $mapdesc and a group
id of $grpid.  The function will mark the group with $parallelflg, 
valid values are 'Y' and 'N'.

=back

=cut

sub create_grp{
  my ($maps_aref,$mapdesc,$grpid,$parallelflag,$dbh) = @_;
  my $counter = 1;
  
  my $sql_cmd = "insert into ps_pf_dl_grp_defn 
              (PF_DL_GRP_ID, PF_DL_GRP_STATUS, DESCR, 
               PF_DL_RUN_PARALLEL, PF_SYS_MAINT_FLG, DESCRLONG)
               values ('$grpid',' ','$mapdesc','$parallelflag',' ',
               '$mapdesc')";
  $dbh->do($sql_cmd);
  
  foreach my $mapname ( @{$maps_aref} ) {
    my $sql_cmd = 
      "INSERT INTO PS_PF_DL_GRP_STEP
        (PF_DL_GRP_ID,PF_DL_ROW_NUM,PF_ODS_SEQ,PF_DL_GRP_ENT_TYP,
        DS_MAPNAME,PF_DL_GRP_ENT_STAT,PF_DL_GRP_EXEC,DESCR,
        PROCESS_INSTANCE,TABLE_APPEND,DATAMAP_COL_SEQ,
        PF_DL_COL_DESCR,PF_SQL_ALIAS,FIELDNAME,PF_DL_LT_JOIN_OPER,
        METAVALUE,PF_SYS_MAINT_FLG,WHERECHUNK) 
        VALUES('$grpid',$counter,$counter,'M','$mapname',' ',' ',' ',
        0,' ',0,' ',' ',' ','=',' ',' ',' ')";
    $counter++;
    if ( defined $dbh->do($sql_cmd) ) { $dbh->commit; }
  }
}

#------------------------------------ release_recsuite

=over 3

=item release_recsuite( $rs_id, $js_id, $dbh )

Specifying a record suite id (e.g. 001) and a jobstream id
(e.g. PS_DL_RUN) will release the recordsuite in the database 
with handle $dbh.

=back

=cut

sub release_recsuite {
  my ( $rs_id, $js_id, $dbh ) = @_;

  my @sql_cmd = 
    ( "update ps_pf_recsuite_tbl set in_use_sw = 'N'
       where recsuite_id = '$rs_id'",

      "Update PS_PF_TEMP_REC_TBL Set PF_MERGE_FLG = 'N',
       PF_RERUN_OVERRIDE = 'N',
       PF_SELECT_WHERE = ' ',
       PF_MERGE_LOCK = 'N'
       where recsuite_id = '$rs_id'",

      "Update PS_PF_TEMP_RL_TBL 
      Set PF_RERUN_OVERRIDE = 'N' 
      where recsuite_id = '$rs_id'",

      "UPDATE PS_PF_JOBSTRM_TBL
       SET JOBSTREAM_STATUS='N',
       job_id=' ',
       business_unit=' ',
       pf_scenario_id=' ',
       run_cntl_id=' ',
       IN_USE_SW='N',
       PROCESS_INSTANCE=0
       WHERE JOBSTREAM_ID='$js_id'
       AND recsuite_id = '$rs_id'",

      "SELECT RECSUITE_ID, TO_CHAR(DTTM_STAMP,
       'YYYY-MM-DD-HH24.MI.SS.\"000000\"'), IN_USE_SW, 
       JOB_ID, PROCESS_INSTANCE, RUN_CNTL_ID, 
       PF_SPAWN_ID, PF_CHUNK_LOCK FROM PS_PF_RECSUITE_TBL
       WHERE RECSUITE_ID='$rs_id' FOR UPDATE OF IN_USE_SW" );

  foreach my $cmd ( @sql_cmd ) {
    if ( ! defined $dbh->do($cmd) ) {
      die "Uh oh!  Failed to execute $cmd\n";
    }
  }
  $dbh->commit;
}
#----------------------------------------------------

=over 3

=item get_maps_aref( $fldr_aref, $dbh )

This function returns a reference to an array that contains 
the names of all the data loader maps associated with any of
the "folders" contained in the array of the first parameter.

=back

=cut

sub get_maps_aref {
  my ( $apps, $dbh ) = @_;
  my ( $maps, @results );

  my $sql_cmd = "select ds_mapname from ps_pf_dl_map_defn where folder_name = 'HR'";

  my $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while( @results = $sth->fetchrow_array ) { 
    push( @{$maps}, $results[0]);
  }
  $sth->finish;

  return( $maps );
}
#------------------------------------------------------------
sub dl_run_seq {
  my ( $fldr_aref, $dbh ) = @_;
  my ( %mappings, @results, @r2, $mapnames );

  my $sql_cmd = "select ds_mapname from ps_pf_dl_map_defn ";
  if ( @{$fldr_aref} == 1 ) {
    $sql_cmd .= "where folder_name = '$$fldr_aref[0]'";
  }
  elsif ( @{$fldr_aref} > 1 ) {
    $sql_cmd .= "where folder_name = '", join "' or folder_name = '", @{$fldr_aref}, "'";
  }
  else { die "You have to supply at least one folder to dl_run_seq"; }

  my $sth = $dbh->prepare($sql_cmd);
  $sth->execute;
  while( @results = $sth->fetchrow_array ) { 
    $mappings{$results[0]} = '';
    $sql_cmd = "select distinct ds_source_rec, edittable from PS_PF_DL_MAPDET_VW
                   where ds_mapname = '$results[0]' and
                   ds_source_rec not like ' ' and
	           edittable not like ' '";
    my $sth2 = $dbh->prepare($sql_cmd);
    $sth2->execute;
    while( @r2 = $sth2->fetchrow_array ) { 
      @results = @{populate_mapnames( $r2[0], 'SRC', \@results, \%mappings, $dbh)};
      @results = @{populate_mapnames( $r2[1], 'LKP', \@results, \%mappings, $dbh)};
    }
    $sth2->finish;

    $sql_cmd = "select distinct lookup_tbl from ps_pf_dl_edt_defn
                where ds_mapname = '$results[0]' 
                and lookup_tbl not like ' '";
    $sth2 = $dbh->prepare($sql_cmd);
    $sth2->execute;
    while( @r2 = $sth2->fetchrow_array ) { 
      @results = @{populate_mapnames( $r2[0], 'EDT', \@results, \%mappings, $dbh)};
    }
    $sth2->finish;
    $sql_cmd = "select distinct lookup_tbl from ps_pf_dl_trn_defn
                where ds_mapname = '$results[0]'
                and lookup_tbl not like ' '";
    $sth2 = $dbh->prepare($sql_cmd);
    $sth2->execute;
    while( @r2 = $sth2->fetchrow_array ) { 
      @results = @{populate_mapnames( $r2[0], 'TRN', \@results, \%mappings, $dbh)};
    }
    $sth2->finish;
  }
  $sth->finish;

  my ( %mn2 );

  foreach my $k ( sort keys %mappings ) {
    foreach my $type qw( SRC LKP TRN ) {
      foreach my $k2 ( keys %{$mappings{$k}{$type}} ) {
	if ( $k eq 'PERSONAL_D00' and $k2 eq 'JOB_F00' ) {next;}
	if ( defined $mappings{$k2} and $k2 ne $k ) { $mn2{$k}{$k2} = ''; }
      }
    }
  }

  print Dumper($mappings{'JOB_F00'});

  my ( @ordered_dl_maps, $k, %done );

  foreach my $map ( sort keys %mappings ) {
    #  print "M: $map\n";
    if ( defined $done{$map} ) { next; }
    push @ordered_dl_maps, dl_recurse( $map, \%mn2, \%done, \@ordered_dl_maps );
    if ( defined $done{$map} ) { next; }
    #  print "PUSHED1: $ordered_dl_maps[-1]\n";
    push @ordered_dl_maps, $map;
    $done{$map} = 1;
    #  print "PUSHED2: $ordered_dl_maps[-1]\n";
  }

  my @uniq;
  my %seen = ();
  foreach my $item ( @ordered_dl_maps ){
    push(@uniq, $item) unless $seen{$item}++;
  }
  return(\@uniq);
}
#------------------------------------------------------------
sub dl_recurse {
  my ( $seed, $mn2, $done, $ordered_dl_maps ) = @_;

  foreach my $k ( keys %{$mn2->{$seed}} ) {
    if ( ! defined $done->{$k} ) { dl_recurse( $k, $mn2, $done, $ordered_dl_maps ); }
  }
  push @{$ordered_dl_maps}, $seed;
  $done->{$seed} = 1;
  return $seed;
}
#------------------------------------------------------------
sub populate_mapnames {
  my ( $obj_name, $obj_type, $results, $mappings, $dbh ) = @_;
  my ( $tbl_aref, $tbl );

  if ( PeopleSoft::Tables::is_view("PS_$obj_name", $dbh) ) {
    $mappings->{$results->[0]}{VIEWS}{$obj_name} = '';
    $tbl_aref = where_from("PS_$obj_name", $dbh);
    if ( defined @{$tbl_aref} ) {
      foreach $tbl ( @{$tbl_aref} ) {
	$tbl =~ s/^PS_//;
	$mappings->{$results->[0]}{$obj_type}{$tbl} = '';
      }
    }
  } else {
    $mappings->{$results->[0]}{$obj_type}{$obj_name} = '';
  }

  return $mappings;
}
