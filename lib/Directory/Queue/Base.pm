#+##############################################################################
#                                                                              #
# File: Directory/Queue/Base.pm                                                #
#                                                                              #
# Description: base class and common code for the Directory::Queue modules     #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Directory::Queue::Base;
use strict;
use warnings;
our $VERSION  = "1.3";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.4 $ =~ /(\d+)\.(\d+)/);

#
# export control
#

use Exporter;
our(@ISA, @EXPORT, @EXPORT_OK, %EXPORT_TAGS);
@ISA = qw(Exporter);
@EXPORT = qw();
@EXPORT_OK = qw(_fatal _name SYSBUFSIZE);
%EXPORT_TAGS = (
    "DIR"  => [qw(_special_mkdir _special_rmdir _special_getdir)],
    "FILE" => [qw(_file_read _file_create _file_write)],
    "RE"   => [qw($_DirectoryRegexp $_ElementRegexp)],
    "ST"   => [qw(ST_DEV ST_INO ST_NLINK ST_MTIME)],
);
Exporter::export_tags();

#
# used modules
#

use POSIX qw(:errno_h :fcntl_h);
use Time::HiRes qw();

#+++############################################################################
#                                                                              #
# Constants                                                                    #
#                                                                              #
#---############################################################################

#
# interesting stat(2) fields
#

use constant ST_DEV   => 0;  # device
use constant ST_INO   => 1;  # inode
use constant ST_NLINK => 3;  # number of hard links
use constant ST_MTIME => 9;  # time of last modification

#
# reasonable buffer size for file I/O operations
#

use constant SYSBUFSIZE => 8192;

#
# regular expressions
#

our(
    $_DirectoryRegexp,    # regexp matching an intermediate directory
    $_ElementRegexp,      # regexp matching an element
);

$_DirectoryRegexp = qr/[0-9a-f]{8}/;
$_ElementRegexp   = qr/[0-9a-f]{14}/;

#+++############################################################################
#                                                                              #
# Common Code                                                                  #
#                                                                              #
#---############################################################################

#
# report a fatal error with a sprintf() API
#

sub _fatal ($@) {
    my($message, @arguments) = @_;

    $message = sprintf($message, @arguments) if @arguments;
    $message =~ s/\s+$//;
    die(caller() . ": $message\n");
}

#
# return the name of a new element to (try to) use with:
#  - 8 hexadecimal digits for the number of seconds since the Epoch
#  - 5 hexadecimal digits for the microseconds part
#  - 1 hexadecimal digit from the pid to further reduce name collisions
#
# properties:
#  - fixed size (14 hexadecimal digits)
#  - likely to be unique (with very high-probability)
#  - can be lexically sorted
#  - ever increasing (for a given process)
#  - reasonably compact
#  - matching $_ElementRegexp
#

sub _name () {
    return(sprintf("%08x%05x%01x", Time::HiRes::gettimeofday(), $$ % 16));
}

#
# create a directory in adversary conditions:
#  - return true on success
#  - return false if the directory already exists
#  - die in case of any other error
#  - handle an optional umask
#

sub _special_mkdir ($$) {
    my($path, $umask) = @_;
    my($oldumask, $success);

    if (defined($umask)) {
	$oldumask = umask($umask);
	$success = mkdir($path);
	umask($oldumask);
    } else {
	$success = mkdir($path);
    }
    return(1) if $success;
    _fatal("cannot mkdir(%s): %s", $path, $!) unless $! == EEXIST and -d $path;
    # RACE: someone else may have created it at the the same time
    return(0);
}

#
# delete a directory in adversary conditions:
#  - return true on success
#  - return false if the path does not exist (anymore)
#  - die in case of any other error
#

sub _special_rmdir ($) {
    my($path) = @_;

    return(1) if rmdir($path);
    _fatal("cannot rmdir(%s): %s", $path, $!) unless $! == ENOENT;
    # RACE: someone else may have deleted it at the the same time
    return(0);
}

#
# get the contents of a directory in adversary conditions:
#  - return the list of names without . and ..
#  - return an empty list if the directory does not exist (anymore),
#    unless the optional second argument is true
#  - die in case of any other error
#

sub _special_getdir ($;$) {
    my($path, $strict) = @_;
    my($dh, @list);

    if (opendir($dh, $path)) {
	@list = grep($_ !~ /^\.\.?$/, readdir($dh));
	closedir($dh) or _fatal("cannot closedir(%s): %s", $path, $!);
	return(@list);
    }
    _fatal("cannot opendir(%s): %s", $path, $!)
	unless $! == ENOENT and not $strict;
    # RACE: someone else may have deleted it at the the same time
    return();
}

#
# read from a file:
#  - return a reference to the file contents
#  - handle optional UTF-8 decoding
#

sub _file_read ($$) {
    my($path, $utf8) = @_;
    my($fh, $data, $done);

    sysopen($fh, $path, O_RDONLY)
	or _fatal("cannot sysopen(%s, O_RDONLY): %s", $path, $!);
    if ($utf8) {
	binmode($fh, ":encoding(utf8)")
	    or _fatal("cannot binmode(%s, :encoding(utf8)): %s", $path, $!);
    } else {
	binmode($fh)
	    or _fatal("cannot binmode(%s): %s", $path, $!);
    }
    $data = "";
    $done = -1;
    while ($done) {
	$done = sysread($fh, $data, SYSBUFSIZE, length($data));
	_fatal("cannot sysread(%s): %s", $path, $!) unless defined($done);
    }
    close($fh) or _fatal("cannot close(%s): %s", $path, $!);
    return(\$data);
}

#
# create a file:
#  - return the file handle on success
#  - tolerate some errors unless the optional third argument is true
#  - die in case of any other error
#  - handle an optional umask
#

sub _file_create ($$;$) {
    my($path, $umask, $strict) = @_;
    my($fh, $oldumask, $success);

    if (defined($umask)) {
	$oldumask = umask($umask);
	$success = sysopen($fh, $path, O_WRONLY|O_CREAT|O_EXCL);
	umask($oldumask);
    } else {
	$success = sysopen($fh, $path, O_WRONLY|O_CREAT|O_EXCL);
    }
    return($fh) if $success;
    _fatal("cannot sysopen(%s, O_WRONLY|O_CREAT|O_EXCL): %s", $path, $!)
	unless ($! == EEXIST or $! == ENOENT) and not $strict;
    # RACE: someone else may have created the file (EEXIST)
    # RACE: the containing directory may be mising (ENOENT)
    return(0);
}

#
# write to a file:
#  - the file must not exist beforehand
#  - this function must be given a reference to the file contents
#  - handle an optional umask
#  - handle optional UTF-8 decoding
#

sub _file_write ($$$$) {
    my($path, $utf8, $umask, $dataref) = @_;
    my($fh, $length, $offset, $done);

    $fh = _file_create($path, $umask, "strict");
    if ($utf8) {
	binmode($fh, ":encoding(utf8)")
	    or _fatal("cannot binmode(%s, :encoding(utf8)): %s", $path, $!);
    } else {
	binmode($fh)
	    or _fatal("cannot binmode(%s): %s", $path, $!);
    }
    $length = length($$dataref);
    $offset = 0;
    while ($length) {
	$done = syswrite($fh, $$dataref, SYSBUFSIZE, $offset);
	_fatal("cannot syswrite(%s): %s", $path, $!) unless defined($done);
	$length -= $done;
	$offset += $done;
    }
    close($fh) or _fatal("cannot close(%s): %s", $path, $!);
}

#+++############################################################################
#                                                                              #
# Base Class                                                                   #
#                                                                              #
#---############################################################################

#
# object creator
#

sub new : method {
    my($class, %option) = @_;
    my($self, $path, $name, @stat);

    # path is mandatory
    _fatal("missing option: path") unless defined($option{path});
    _fatal("not a directory: %s", $option{path})
	if -e $option{path} and not -d _;
    # build the object
    $self = {
	path => $option{path},	# toplevel path
	dirs => [],		# cached list of intermediate directories
	elts => [],		# cached list of elements
    };
    # handle the umask option
    if (defined($option{umask})) {
	_fatal("invalid umask: %s", $option{umask})
	    unless $option{umask} =~ /^\d+$/ and $option{umask} < 512;
	$self->{umask} = $option{umask};
    }
    # create the toplevel directory if needed
    $path = "";
    foreach $name (split(/\/+/, $self->{path})) {
	$path .= $name . "/";
	_special_mkdir($path, $self->{umask}) unless -d $path;
    }
    # store the queue unique identifier
    if ($^O =~ /^(cygwin|dos|MSWin32)$/) {
	# we cannot rely on inode number :-(
	$self->{id} = $self->{path};
    } else {
	# device number plus inode number should be unique
	@stat = stat($self->{path});
	_fatal("cannot stat(%s): %s", $self->{path}, $!) unless @stat;
	$self->{id} = $stat[ST_DEV] . ":" . $stat[ST_INO];
    }
    # that's it!
    bless($self, $class);
    return($self);
}

#
# copy/clone the object
#
# note:
#  - the main purpose is to copy/clone the iterator cached state
#  - the other attributes are _not_ cloned but this is not a problem
#    since they should not change
#

sub copy : method {
    my($self) = @_;
    my($copy);

    $copy = { %$self };
    $copy->{dirs} = [];
    $copy->{elts} = [];
    bless($copy, ref($self));
    return($copy);
}

#
# return the toplevel path of the queue
#

sub path : method {
    my($self) = @_;

    return($self->{path});
}

#
# return a unique identifier for the queue
#

sub id : method {
    my($self) = @_;

    return($self->{id});
}

#
# return the name of the next element in the queue, using cached information
#

sub next : method {
    my($self) = @_;
    my($dir, $name, @list);

    return(shift(@{ $self->{elts} })) if @{ $self->{elts} };
    while (@{ $self->{dirs} }) {
	$dir = shift(@{ $self->{dirs} });
	foreach $name (_special_getdir($self->{path} . "/" . $dir)) {
	    push(@list, $1) if $name =~ /^($_ElementRegexp)$/o; # untaint
	}
	next unless @list;
	$self->{elts} = [ map("$dir/$_", sort(@list)) ];
	return(shift(@{ $self->{elts} }));
    }
    return("");
}

#
# return the first element in the queue and cache information about the next ones
#

sub first : method {
    my($self) = @_;
    my($name, @list);

    foreach $name (_special_getdir($self->{path}, "strict")) {
	push(@list, $1) if $name =~ /^($_DirectoryRegexp)$/o; # untaint
    }
    $self->{dirs} = [ sort(@list) ];
    $self->{elts} = [];
    return($self->next());
}

#
# touch an element to indicate that it is still being used
#

sub touch : method {
    my($self, $element) = @_;
    my($time, $path);

    $time = time();
    $path = $self->{path} . "/" . $element;
    utime($time, $time, $path)
	or _fatal("cannot utime(%d, %d, %s): %s", $time, $time, $path, $!);
}

1;

__END__

=head1 NAME

Directory::Queue::Base - base class and common code for the Directory::Queue modules

=head1 DESCRIPTION

This module provides a base class as well as common code for the
Directory::Queue modules.

It is used internally by the Directory::Queue modules and should not
be used elsewhere.

=head1 METHODS

Here are the methods available in the base class:

=over

=item new(PATH)

return a new object (class method)

=item copy()

return a copy of the object

=item path()

return the queue toplevel path

=item id()

return a unique identifier for the queue

=item first()

return the first element in the queue, resetting the iterator;
return an empty string if the queue is empty

=item next()

return the next element in the queue, incrementing the iterator;
return an empty string if there is no next element

=item touch(ELEMENT)

update the element's access and modification times to indicate that it
is still being used

=back

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright CERN 2010-2011
