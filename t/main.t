#!perl -T

use strict;
use warnings;
use Encode;
use Directory::Queue;
use Test::More tests => 8;
use File::Temp qw(tempdir);
use POSIX qw(:errno_h :fcntl_h);

use constant STR_ISO     => "Théâtre Français";
use constant STR_UNICODE => "is \x{263A}?";

our($tmpdir, $dq, $elt, @list);

sub dirlist ($) {
    my($path) = @_;
    my($dh, @list);

    opendir($dh, $path) or die("cannot opendir($path): $!\n");
    @list = grep($_ !~ /^\.\.?$/, readdir($dh));
    closedir($dh) or die("cannot closedir($path): $!\n");
    return(@list);
}

sub contents ($) {
    my($path) = @_;
    my($fh, $contents, $done);

    sysopen($fh, $path, O_RDONLY) or die("cannot sysopen($path): $!\n");
    binmode($fh) or die("cannot binmode($path): $!\n");
    $contents = "";
    $done = -1;
    while ($done) {
	$done = sysread($fh, $contents, 8192, length($contents));
	die("cannot sysread($path): $!\n") unless defined($done);
    }
    close($fh) or die("cannot close($path): $!\n");
    return($contents);
}

$tmpdir = tempdir(CLEANUP => 1);
#diag("Using temporary directory $tmpdir");

@list = dirlist($tmpdir);
ok(@list == 0, "empty directory");

$dq = Directory::Queue->new(path => $tmpdir, "schema" => { string => "string" });
@list = sort(dirlist($tmpdir));
is("@list", "obsolete temporary", "empty queue");

$elt = $dq->add(string => STR_ISO);
@list = sort(dirlist($tmpdir));
is("@list", "00000000 obsolete temporary", "non-empty queue");
@list = dirlist("$tmpdir/00000000");
is("00000000/@list", $elt, "one element");
is(contents("$tmpdir/$elt/string"), encode("UTF-8", STR_ISO), "ISO-8859-1 string");

$elt = $dq->add(string => STR_UNICODE);
is(contents("$tmpdir/$elt/string"), encode("UTF-8", STR_UNICODE), "Unicode string");

ok($dq->count() == 2, "count");

$elt = $dq->first();
$dq->lock($elt) or die;
$dq->remove($elt);
ok($dq->count() == 1, "remove");
