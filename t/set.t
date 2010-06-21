#!perl -T

use strict;
use warnings;
use Directory::Queue;
use Directory::Queue::Set;
use Test::More tests => 10;
use File::Temp qw(tempdir);

our($tmpdir, $dq1, $dq2, $dqs, $dq, $elt);

$tmpdir = tempdir(CLEANUP => 1);
#diag("Using temporary directory $tmpdir");

$dq1 = Directory::Queue->new(path => "$tmpdir/1", "schema" => { string => "string" });
$dq2 = Directory::Queue->new(path => "$tmpdir/2", "schema" => { string => "string" });
isnt($dq1->path(), $dq2->path(), "path");
isnt($dq1->id(), $dq2->id(), "id");
is($dq1->id(), $dq1->copy()->id(), "copy");

$dq1->add(string => "test dq1.1");
$dq2->add(string => "test dq2.1");
$dq1->add(string => "test dq1.2");

$dqs = Directory::Queue::Set->new($dq1, $dq2);
$dqs->remove($dq1);
$dqs->add($dq1);
is($dqs->count(), "3", "count all");

($dq, $elt) = $dqs->first();
is($dq->id(), $dq1->id(), "first");
$dq->lock($elt) and $dq->remove($elt);

($dq, $elt) = $dqs->next();
is($dq->id(), $dq2->id(), "next");
$dq->lock($elt) and $dq->remove($elt);

($dq, $elt) = $dqs->next();
is($dq->id(), $dq1->id(), "last");

($dq, $elt) = $dqs->next();
ok(!defined($dq), "end");
is($dq1->count(), "1", "count 1");
is($dq2->count(), "0", "count 2");
