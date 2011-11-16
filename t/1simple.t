#!perl

use strict;
use warnings;
use Directory::Queue::Simple;
use Test::More tests => 20;
use File::Temp qw(tempdir);

our($tmpdir, $dq, $elt, @list, $time, $tmp);

sub dirlist ($) {
    my($path) = @_;
    my($dh, @list);

    opendir($dh, $path) or die("cannot opendir($path): $!\n");
    @list = grep($_ !~ /^\.\.?$/, readdir($dh));
    closedir($dh) or die("cannot closedir($path): $!\n");
    return(@list);
}

$tmpdir = tempdir(CLEANUP => 1);

@list = dirlist($tmpdir);
is(scalar(@list), 0, "empty directory");

$dq = Directory::Queue::Simple->new(path => $tmpdir);
is(scalar(@list), 0, "empty queue");

$elt = $dq->add("hello world");
@list = dirlist($tmpdir);
is(scalar(@list), 1, "queue one element (1)");
ok($list[0] =~ /^[0-9a-f]{8}$/, "queue one element (2)");
@list = dirlist("$tmpdir/$list[0]");
is(scalar(@list), 1, "queue one element (3)");
ok($list[0] =~ /^[0-9a-f]{14}$/, "queue one element (4)");
is($dq->count(), 1, "queue one element (5)");

foreach (1 .. 12) {
    $elt = $dq->add($_);
}
is($dq->count(), 13, "count (1)");

$elt = $dq->first();
ok($elt, "first");
$elt = $dq->next();
ok($elt, "next");

ok($dq->lock($elt), "lock");
ok($dq->unlock($elt), "unlock");

ok($dq->lock($elt), "lock");
eval { $dq->remove($elt) };
is($@, "", "remove (1)");
is($dq->count(), 12, "count (2)");

$elt = $dq->next();
eval { $dq->remove($elt) };
ok($@, "remove (2)");

for ($elt = $dq->first(); $elt; $elt = $dq->next()) {
    $dq->lock($elt) and $dq->remove($elt);
}
is($dq->count(), 0, "count (3)");

$elt = $dq->add("dummy");
$tmp = "$tmpdir/$elt.tmp";
rename("$tmpdir/$elt", $tmp) or die("cannot rename($tmpdir/$elt, $tmp): $!\n");
is($dq->count(), 0, "count (4)");
$time = time() - 1000;
utime($time, $time, $tmp) or die("cannot utime($time, $time, $tmp): $!\n");
$tmp = 0;
{
    local $SIG{__WARN__} = sub { $tmp++ if $_[0] =~ /removing too old/ };
    $dq->purge(maxtemp => 5);
}
is($tmp, 1, "purge (1)");
$elt =~ s/\/.+//;
@list = dirlist("$tmpdir/$elt");
is(scalar(@list), 0, "purge (2)");
