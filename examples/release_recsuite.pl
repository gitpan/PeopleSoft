#!/usr/bin/perl
#
use PeopleSoft::EPM::DataLoader;
use PeopleSoft::Tables;
use strict;
use Data::Dumper;

my $dbh = get_dbh('sysadm','rclc1891','eproto88');

# If you're still wedged for a particular map 
# check pf_ods_status = 'I' in PS_PF_DL_CONTROL

release_recsuite('001', 'PF_DL_RUN', $dbh);
release_recsuite('002', 'PF_DL_RUN', $dbh);
release_recsuite('003', 'PF_DL_RUN', $dbh);

$dbh->disconnect;
exit;
