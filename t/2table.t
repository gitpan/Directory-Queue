#!perl

use strict;
use warnings;
use Directory::Queue;
use Test::More tests => 48;
use File::Temp qw(tempdir);

sub check_hash ($$$) {
    my($hash1, $hash2, $text) = @_;
    my($tmp1, $tmp2);

    $tmp1 = join("+", sort(keys(%$hash1)));
    $tmp2 = join("+", sort(keys(%$hash2)));
    is($tmp1, $tmp2, "$text (keys)");
    $tmp1 = join("+", map($hash1->{$_}, sort(keys(%$hash1))));
    $tmp2 = join("+", map($hash2->{$_}, sort(keys(%$hash2))));
    is($tmp1, $tmp2, "$text (values)");
}

sub check_elt ($$$$) {
    my($data, $dq, $elt, $text) = @_;
    my(@list, $scalar);

    $dq->lock($elt) or die;
    # list
    @list = $dq->get($elt);
    is(scalar(@list), 2, "$text - get() 1");
    is($list[0], "table", "$text - get() 2");
    check_hash($data, $list[1], "$text - get()");
    # scalar
    $scalar = $dq->get($elt);
    is(ref($scalar), "HASH", "$text - get{} 1");
    @list = keys(%$scalar);
    is("@list", "table", "$text - get{} 2");
    check_hash($data, $scalar->{table}, "$text - get{}");
}

sub check_data ($$$) {
    my($data, $dq, $text) = @_;
    my($elt);

    $elt = $dq->add(table => $data);
    check_elt($data, $dq, $elt, "$text - add()");
    $elt = $dq->add({ table => $data });
    check_elt($data, $dq, $elt, "$text - add{}");
}

our($tmpdir, $dq);

$tmpdir = tempdir(CLEANUP => 1);
$dq = Directory::Queue->new(path => $tmpdir, schema => { table => "table" });
check_data({}, $dq, "empty");
check_data({ "" => "", "abc" => "", "" => "def"}, $dq, "zero");
check_data({foo => 1, bar => 2}, $dq, "normal");
