#+##############################################################################
#                                                                              #
# File: Directory/Queue.pm                                                     #
#                                                                              #
# Description: object oriented interface to a directory based queue            #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Directory::Queue;
use strict;
use warnings;
our $VERSION = sprintf("%d.%02d", q$Revision: 1.25 $ =~ /(\d+)\.(\d+)/);

#
# used modules
#

use POSIX qw(:errno_h :fcntl_h);
use Time::HiRes qw();

#
# constants
#

# stat(2) fields
use constant ST_DEV   => 0;  # device
use constant ST_INO   => 1;  # inode
use constant ST_NLINK => 3;  # number of hard links
use constant ST_MTIME => 9;  # time of last modification

# name of the directory holding temporary elements
use constant TEMPORARY_DIRECTORY => "temporary";

# name of the directory holding obsolete elements
use constant OBSOLETE_DIRECTORY => "obsolete";

# name of the directory indicating a locked element
use constant LOCKED_DIRECTORY => "locked";

# states returned by the _state() method
use constant STATE_UNLOCKED => "U";
use constant STATE_LOCKED   => "L";
use constant STATE_MISSING  => "M";

# reasonable buffer size for file I/O operations
use constant SYSBUFSIZE => 8192;

#
# global variables
#

our(
    $_DirectoryRegexp,    # regexp matching an intermediate directory
    $_ElementRegexp,      # regexp matching an element
    $_FileRegexp,	  # regexp matching a file in an element directory
    %_Byte2Esc,           # byte to escape map
    %_Esc2Byte,           # escape to byte map
);

$_DirectoryRegexp = qr/[0-9a-f]{8}/;
$_ElementRegexp   = qr/[0-9a-f]{14}/;
$_FileRegexp      = qr/[0-9a-zA-Z]+/;

%_Byte2Esc = ("\x5c" => "\\\\", "\x09" => "\\t", "\x0a" => "\\n");
%_Esc2Byte = reverse(%_Byte2Esc);

#+++############################################################################
#                                                                              #
# Helper Functions                                                             #
#                                                                              #
#---############################################################################

#
# report a fatal error
#

sub _fatal ($@) {
    my($format, @arguments) = @_;
    my($message);

    $message = sprintf($format, @arguments);
    $message =~ s/\s+$//;
    die(caller() . ": $message\n");
}

#
# read from a file
#

sub _file_read ($$) {
    my($path, $utf8) = @_;
    my($fh, $contents, $done);

    sysopen($fh, $path, O_RDONLY)
	or _fatal("cannot sysopen(%s, O_RDONLY): %s", $path, $!);
    if ($utf8) {
	binmode($fh, ":encoding(utf8)")
	    or _fatal("cannot binmode(%s, :encoding(utf8)): %s", $path, $!);
    } else {
	binmode($fh)
	    or _fatal("cannot binmode(%s): %s", $path, $!);
    }
    $contents = "";
    $done = -1;
    while ($done) {
	$done = sysread($fh, $contents, SYSBUFSIZE, length($contents));
	_fatal("cannot sysread(%s): %s", $path, $!) unless defined($done);
    }
    close($fh) or _fatal("cannot close(%s): %s", $path, $!);
    return($contents);
}

#
# write to a file
#

sub _file_write ($$$$) {
    my($path, $utf8, $umask, $contents) = @_;
    my($fh, $oldumask, $success, $length, $offset, $done);

    if (defined($umask)) {
	$oldumask = umask($umask);
	$success = sysopen($fh, $path, O_WRONLY|O_CREAT|O_EXCL);
	umask($oldumask);
    } else {
	$success = sysopen($fh, $path, O_WRONLY|O_CREAT|O_EXCL);
    }
    $success or _fatal("cannot sysopen(%s, O_WRONLY|O_CREAT|O_EXCL): %s", $path, $!);
    if ($utf8) {
	binmode($fh, ":encoding(utf8)")
	    or _fatal("cannot binmode(%s, :encoding(utf8)): %s", $path, $!);
    } else {
	binmode($fh)
	    or _fatal("cannot binmode(%s): %s", $path, $!);
    }
    $length = length($contents);
    $offset = 0;
    while ($length) {
	$done = syswrite($fh, $contents, SYSBUFSIZE, $offset);
	_fatal("cannot syswrite(%s): %s", $path, $!) unless defined($done);
	$length -= $done;
	$offset += $done;
    }
    close($fh) or _fatal("cannot close(%s): %s", $path, $!);
}

#
# transform a hash of strings into a string
#
# note:
#  - the keys are sorted so that identical hashes yield to identical strings
#

sub _hash2string ($) {
    my($hash) = @_;
    my($key, $value, $string);

    $string = "";
    foreach $key (sort(keys(%$hash))) {
	$value = $hash->{$key};
	_fatal("undefined hash value: %s", $key) unless defined($value);
	_fatal("invalid hash scalar: %s", $value) if ref($value);
	$key   =~ s/([\x5c\x09\x0a])/$_Byte2Esc{$1}/g;
	$value =~ s/([\x5c\x09\x0a])/$_Byte2Esc{$1}/g;
	$string .= $key . "\x09" . $value . "\x0a";
    }
    return($string);
}

#
# transform a string into a hash of strings
#
# note:
#  - duplicate keys are not checked (the last one wins)
#

sub _string2hash ($) {
    my($string) = @_;
    my($line, $key, $value, %hash);

    foreach $line (split(/\x0a/, $string)) {
	_fatal("unexpected hash line: %s", $line)
	    unless $line =~ /^([^\x09\x0a]*)\x09([^\x09\x0a]*)$/o;
	($key, $value) = ($1, $2);
	$key   =~ s/(\\[\\tn])/$_Esc2Byte{$1}/g;
	$value =~ s/(\\[\\tn])/$_Esc2Byte{$1}/g;
	$hash{$key} = $value;
    }
    return(\%hash);
}

#
# get the contents of a directory as a list of names, without . and ..
#
# note:
#  - if the optional second argument is true, it is not an error if the
#    directory does not exist (anymore)
#

sub _directory_contents ($;$) {
    my($path, $missingok) = @_;
    my($dh, @list);

    unless (opendir($dh, $path)) {
	_fatal("cannot opendir(%s): %s", $path, $!)
	    unless $missingok and $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return();
    }
    @list = grep($_ !~ /^\.\.?$/, readdir($dh));
    closedir($dh) or _fatal("cannot closedir(%s): %s", $path, $!);
    return(@list);
}

#
# check if a path is old enough:
#  - return true if the path exists and is (strictly) older than the given time
#  - return false if it does not exist or it is newer
#  - die in case of any other error
#
# note:
#  - lstat() is used so symlinks are not followed
#

sub _older ($$) {
    my($path, $time) = @_;
    my(@stat);

    @stat = lstat($path);
    unless (@stat) {
	_fatal("cannot lstat(%s): %s", $path, $!) unless $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return(0);
    }
    return($stat[ST_MTIME] < $time);
}

#
# count the number of sub-directories in the given directory:
#  - return undef if the directory does not exist (anymore)
#  - die in case of any other error
#
# note:
#  - lstat() is used so symlinks are not followed
#  - this only checks the number of links
#  - we do not even check that the path indeed points to a directory!
#

sub _subdirs ($) {
    my($path) = @_;
    my(@stat);

    @stat = lstat($path);
    unless (@stat) {
	_fatal("cannot lstat(%s): %s", $path, $!) unless $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return();
    }
    return($stat[ST_NLINK] - 2) unless $^O =~ /^(cygwin|dos|MSWin32)$/;
    # argh! we cannot rely on the number of links on Windows :-(
    return(scalar(_directory_contents($path, 1)));
}

#
# create a directory:
#  - return true on success
#  - return false if something with the same path already exists
#  - die in case of any other error
#
# note:
#  - in case something with the same path already exists, we do not check
#    that this is indeed a directory as this should always be the case here
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
    unless ($success) {
	_fatal("cannot mkdir(%s): %s", $path, $!) unless $! == EEXIST;
	# RACE: this path (now) exists
	return(0);
    }
    return(1);
}

#
# delete a directory:
#  - return true on success
#  - return false if the path does not exist (anymore)
#  - die in case of any other error
#

sub _special_rmdir ($) {
    my($path) = @_;

    unless (rmdir($path)) {
	_fatal("cannot rmdir(%s): %s", $path, $!) unless $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return(0);
    }
    return(1);
}

#
# return the name of a new element to (try to) use with:
#  - 8 hexadecimal digits for the number of seconds since the Epoch
#  - 5 hexadecimal digits for the microseconds part
#  - 1 hexadecimal digit from the pid to further reduce name collisions
#
# properties:
#  - fixed size (14 hexadecimal digits)
#  - likely to be unique (with high-probability)
#  - can be lexically sorted
#  - ever increasing (for a given process)
#  - reasonably compact
#  - matching $_ElementRegexp
#

sub _new_name () {
    return(sprintf("%08x%05x%01x", Time::HiRes::gettimeofday(), $$ % 16));
}

#
# check the given string to make sure it represents a valid element name
#

sub _check_element ($) {
    my($element) = @_;

    _fatal("invalid element: %s", $element)
	unless $element =~ m/^($_DirectoryRegexp)\/($_ElementRegexp)$/o;
}

#+++############################################################################
#                                                                              #
# Object Oriented Interface                                                    #
#                                                                              #
#---############################################################################

#
# object constructor
#

sub new : method {
    my($class, %option) = @_;
    my($self, $name, $path, @stat);

    # default object
    $self = {
	dirs => [],	      # cached list of intermediate directories
	elts => [],	      # cached list of elements
	maxelts => 16_000,    # maximum number of elements allowed per directory
    };
    # check options
    _fatal("missing option: path") unless $option{path};
    foreach $name (qw(path umask maxelts)) {
 	next unless defined($option{$name});
	$self->{$name} = delete($option{$name});
	_fatal("invalid %s: %s", $name, $self->{$name})
	    if ref($self->{$name});
    }
    # check umask
    if (defined($self->{umask})) {
	_fatal("invalid umask: %s", $self->{umask})
	    unless $self->{umask} =~ /^\d+$/ and $self->{umask} < 512;
    }
    # check maxelts
    if (defined($self->{maxelts})) {
	_fatal("invalid maxelts: %s", $self->{maxelts})
	    unless $self->{maxelts} =~ /^\d+$/ and $self->{maxelts} > 0;
    }
    # check schema
    if ($option{schema}) {
	_fatal("invalid schema: %s", $option{schema})
	    unless ref($option{schema}) eq "HASH";
	foreach $name (keys(%{ $option{schema} })) {
	    _fatal("invalid schema name: %s", $name)
		unless $name =~ /^($_FileRegexp)$/ and $name ne LOCKED_DIRECTORY;
	    _fatal("invalid schema type: %s", $option{schema}{$name})
		unless $option{schema}{$name} =~ /^(binary|string|table)(\?)?$/;
	    $self->{type}{$name} = $1;
	    $self->{mandatory}{$name} = 1 unless $2;
	}
	_fatal("invalid schema: no mandatory data")
	    unless $self->{mandatory};
	delete($option{schema});
    }
    # check unexpected options
    foreach $name (keys(%option)) {
	_fatal("unexpected option: %s", $name);
    }
    bless($self, $class);
    # create toplevel directory
    $path = "";
    foreach $name (split(/\/+/, $self->{path})) {
	$path .= $name . "/";
	_special_mkdir($path, $self->{umask}) unless -d $path;
    }
    # create other directories
    foreach $name (TEMPORARY_DIRECTORY, OBSOLETE_DIRECTORY) {
	$path = $self->{path} . "/" . $name;
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
    # so far so good...
    return($self);
}

#
# copy/clone the object
#
# note:
#  - the main purpose is to copy/clone the iterator cached state
#  - the other structured attributes (including schema) are not cloned
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
# return the queue toplevel path
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
# return the state of the given element
#
# note:
#  - this is only an indication as the state may be changed by an other process
#

sub _state : method {
    my($self, $element) = @_;
    my($path);

    $path = $self->{path} . "/" . $element;
    return(STATE_LOCKED)   if -d $path . "/" . LOCKED_DIRECTORY;
    return(STATE_UNLOCKED) if -d $path;
    # the element does not exist (anymore)
    return(STATE_MISSING);
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
	foreach $name (_directory_contents($self->{path} . "/" . $dir, 1)) {
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

    foreach $name (_directory_contents($self->{path})) {
	push(@list, $1) if $name =~ /^($_DirectoryRegexp)$/o; # untaint
    }
    $self->{dirs} = [ sort(@list) ];
    $self->{elts} = [];
    return($self->next());
}

#
# return the number of elements in the queue, regardless of their state
#

sub count : method {
    my($self) = @_;
    my($count, $name, @list, $subdirs);

    $count = 0;
    # get the list of existing directories
    foreach $name (_directory_contents($self->{path})) {
	push(@list, $1) if $name =~ /^($_DirectoryRegexp)$/o; # untaint
    }
    # count sub-directories
    foreach $name (@list) {
	$subdirs = _subdirs($self->{path} . "/" . $name);
	$count += $subdirs if $subdirs;
    }
    # that's all
    return($count);
}

#
# lock an element:
#  - return true on success
#  - return false in case the element could not be locked (in permissive mode)
#
# note:
#  - locking can fail:
#     - if the element has been locked by somebody else (EEXIST)
#     - if the element has been removed by somebody else (ENOENT)
#  - if the optional second argument is true, it is not an error if
#    the element cannot be locked (= permissive mode), this is the default
#    as one usually cannot be sure that nobody else will try to lock it
#  - the directory's mtime will change automatically (after a successful mkdir()),
#    this will later be used to detect stalled locks
#

sub lock : method {
    my($self, $element, $permissive) = @_;
    my($path, $oldumask, $success);

    _check_element($element);
    $permissive = 1 unless defined($permissive);
    $path = $self->{path} . "/" . $element . "/" . LOCKED_DIRECTORY;
    if (defined($self->{umask})) {
	$oldumask = umask($self->{umask});
	$success = mkdir($path);
	umask($oldumask);
    } else {
	$success = mkdir($path);
    }
    unless ($success) {
	if ($permissive) {
	    # RACE: the locked directory already exists
	    return(0) if $! == EEXIST;
	    # RACE: the element directory does not exist anymore
	    return(0) if $! == ENOENT;
	}
	# otherwise this is unexpected...
	_fatal("cannot mkdir(%s): %s", $path, $!);
    }
    $path = $self->{path} . "/" . $element;
    unless (lstat($path)) {
	if ($permissive) {
	    # RACE: the element directory does not exist anymore
	    # (this can happen if an other process locked & removed the element)
	    return(0) if $! == ENOENT;
	}
	# otherwise this is unexpected...
	_fatal("cannot lstat(%s): %s", $path, $!);
    }
    # so far so good
    return(1);
}

#
# unlock an element:
#  - return true on success
#  - return false in case the element could not be unlocked (in permissive mode)
#
# note:
#  - unlocking can fail:
#     - if the element has been unlocked by somebody else (ENOENT)
#     - if the element has been removed by somebody else (ENOENT)
#  - if the optional second argument is true, it is not an error if
#    the element cannot be unlocked (= permissive mode), this is _not_ the default
#    as unlock() should normally be called by whoever locked the element
#

sub unlock : method {
    my($self, $element, $permissive) = @_;
    my($path);

    _check_element($element);
    $path = $self->{path} . "/" . $element . "/" . LOCKED_DIRECTORY;
    unless (rmdir($path)) {
	if ($permissive) {
	    # RACE: the element directory or its lock does not exist anymore
	    return(0) if $! == ENOENT;
	}
	# otherwise this is unexpected...
	_fatal("cannot rmdir(%s): %s", $path, $!);
    }
    # so far so good
    return(1);
}

#
# remove a locked element from the queue
#

sub remove : method {
    my($self, $element) = @_;
    my($temp, $name, $path);

    _check_element($element);
    _fatal("cannot remove %s: not locked", $element)
	unless $self->_state($element) eq STATE_LOCKED;
    # move the element out of its intermediate directory
    $path = $self->{path} . "/" . $element;
    while (1) {
	$temp = $self->{path} . "/" . OBSOLETE_DIRECTORY . "/" . _new_name();
	rename($path, $temp) and last;
	_fatal("cannot rename(%s, %s): %s", $path, $temp, $!)
	    unless $! == ENOTEMPTY or $! == EEXIST;
	# RACE: the target directory was already present...
    }
    # remove the data files
    foreach $name (_directory_contents($temp)) {
	next if $name eq LOCKED_DIRECTORY;
	_fatal("unexpected file in %s: %s", $temp, $name)
	    unless $name =~ /^($_FileRegexp)$/;
	$path = $temp . "/" . $1; # untaint
	unlink($path) and next;
	_fatal("cannot unlink(%s): %s", $path, $!);
    }
    # remove the locked directory
    $path = $temp . "/" . LOCKED_DIRECTORY;
    while (1) {
	rmdir($path) or _fatal("cannot rmdir(%s): %s", $path, $!);
	rmdir($temp) and return;
	_fatal("cannot rmdir(%s): %s", $temp, $!)
	    unless $! == ENOTEMPTY or $! == EEXIST;
	# RACE: this can happen if an other process managed to lock this element
	# while it was being removed so we try again to remove the lock
    }
}

#
# get an element from a locked element
#

sub get : method {
    my($self, $element) = @_;
    my(%data, $name, $path);

    _fatal("unknown schema") unless $self->{type};
    _check_element($element);
    _fatal("cannot get %s: not locked", $element)
	unless $self->_state($element) eq STATE_LOCKED;
    foreach $name (keys(%{ $self->{type} })) {
	$path = "$self->{path}/$element/$name";
	unless (lstat($path)) {
	    _fatal("cannot lstat(%s): %s", $path, $!) unless $! == ENOENT;
	    if ($self->{mandatory}{$name}) {
		_fatal("missing data file: %s", $path);
	    } else {
		next;
	    }
	}
	if ($self->{type}{$name} eq "binary") {
	    $data{$name} = _file_read($path, 0);
	} elsif ($self->{type}{$name} eq "string") {
	    $data{$name} = _file_read($path, 1);
	} elsif ($self->{type}{$name} eq "table") {
	    $data{$name} = _string2hash(_file_read($path, 1));
	} else {
	    _fatal("unexpected data type: %s", $self->{type}{$name});
	}
    }
    return(%data);
}

#
# return the name of the intermediate directory that can be used for insertion:
#  - if there is none, an initial one will be created
#  - if it is full, a new one will be created
#  - in any case the name will match $_DirectoryRegexp
#

sub _insertion_directory : method {
    my($self) = @_;
    my(@list, $name, $subdirs);

    # get the list of existing directories
    foreach $name (_directory_contents($self->{path})) {
	push(@list, $1) if $name =~ /^($_DirectoryRegexp)$/o; # untaint
    }
    # handle the case with no directories yet
    unless (@list) {
	$name = sprintf("%08x", 0);
	_special_mkdir($self->{path} . "/" . $name, $self->{umask});
	return($name);
    }
    # check the last directory
    @list = sort(@list);
    $name = pop(@list);
    $subdirs = _subdirs($self->{path} . "/" . $name);
    if (defined($subdirs)) {
	return($name) if $subdirs < $self->{maxelts};
	# this last directory is now full... create a new one
    } else {
	# RACE: at this point, the directory does not exist anymore, so it
	# must have been purged after we listed the directory contents...
	# we do not try to do more and simply create a new directory
    }
    # we need a new directory
    $name = sprintf("%08x", hex($name) + 1);
    _special_mkdir($self->{path} . "/" . $name, $self->{umask});
    return($name);
}

#
# add a new element to the queue and return its name
#
# note:
#  - the destination directory must _not_ be created beforehand as it would
#    be seen as a valid (but empty) element directory by an other process,
#    we therefor use rename() from a temporary directory
#  - syswrite() used in _file_write() may die with a "Wide character"
#    "severe warning", we trap it here to provide better information
#

sub add : method {
    my($self, %data) = @_;
    my($temp, $dir, $name, $path);

    _fatal("unknown schema") unless $self->{type};
    while (1) {
	$temp = $self->{path} . "/" . TEMPORARY_DIRECTORY . "/" . _new_name();
	last if _special_mkdir($temp, $self->{umask});
    }
    foreach $name (keys(%data)) {
	_fatal("unexpected data: %s", $name) unless $self->{type}{$name};
	if ($self->{type}{$name} eq "binary") {
	    _fatal("unexpected binary data in %s: %s", $name, $data{$name})
		if ref($data{$name});
	    eval {
		_file_write("$temp/$name", 0, $self->{umask}, $data{$name});
	    };
	} elsif ($self->{type}{$name} eq "string") {
	    _fatal("unexpected string data in %s: %s", $name, $data{$name})
		if ref($data{$name});
	    eval {
		_file_write("$temp/$name", 1, $self->{umask}, $data{$name});
	    };
	} elsif ($self->{type}{$name} eq "table") {
	    _fatal("unexpected table data in %s: %s", $name, $data{$name})
		unless ref($data{$name}) eq "HASH";
	    eval {
		_file_write("$temp/$name", 1, $self->{umask}, _hash2string($data{$name}));
	    };
	} else {
	    _fatal("unexpected data type in %s: %s", $name, $self->{type}{$name});
	}
	if ($@) {
	    if ($@ =~ /^Wide character in /) {
		_fatal("unexpected wide character in %s: %s", $name, $data{$name});
	    } else {
		die($@);
	    }
	}
    }
    foreach $name (keys(%{ $self->{mandatory} })) {
	_fatal("missing mandatory data: %s", $name)
	    unless defined($data{$name});
    }
    $dir = $self->_insertion_directory();
    while (1) {
	$name = $dir . "/" . _new_name();
	$path = $self->{path} . "/" . $name;
	rename($temp, $path) and return($name);
	_fatal("cannot rename(%s, %s): %s", $temp, $path, $!)
	    unless $! == ENOTEMPTY or $! == EEXIST;
	# RACE: the target directory was already present...
    }
}

#
# return the list of volatile (i.e. temporary or obsolete) directories
#

sub _volatile : method {
    my($self) = @_;
    my(@list, $name);

    foreach $name (_directory_contents($self->{path} . "/" . TEMPORARY_DIRECTORY, 1)) {
	push(@list, TEMPORARY_DIRECTORY . "/" . $1)
	    if $name =~ /^($_ElementRegexp)$/o; # untaint
    }
    foreach $name (_directory_contents($self->{path} . "/" . OBSOLETE_DIRECTORY, 1)) {
	push(@list, OBSOLETE_DIRECTORY . "/" . $1)
	    if $name =~ /^($_ElementRegexp)$/o; # untaint
    }
    return(@list);
}

#
# purge the queue:
#  - delete unused intermediate directories
#  - delete too old temporary directories
#  - unlock too old locked directories
#
# note: this uses first()/next() to iterate so this will reset the cursor
#

sub purge : method {
    my($self, %option) = @_;
    my(@list, $name, $path, $subdirs, $oldtime, $file, $fpath);

    # check options
    $option{maxtemp} = 300 unless defined($option{maxtemp});
    $option{maxlock} = 600 unless defined($option{maxlock});
    foreach $name (keys(%option)) {
	_fatal("unexpected option: %s", $name)
	    unless $name =~ /^(maxtemp|maxlock)$/;
	_fatal("invalid %s: %s", $name, $option{$name})
	    unless $option{$name} =~ /^\d+$/;
    }
    # get the list of intermediate directories
    @list = ();
    foreach $name (_directory_contents($self->{path})) {
	push(@list, $1) if $name =~ /^($_DirectoryRegexp)$/o; # untaint
    }
    @list = sort(@list);
    # try to purge all but last one
    if (@list > 1) {
	pop(@list);
	foreach $name (@list) {
	    $path = $self->{path} . "/" . $name;
	    $subdirs = _subdirs($path);
	    next if $subdirs or not defined($subdirs);
	    _special_rmdir($path);
	}
    }
    # remove the volatile directories which are too old
    if ($option{maxtemp}) {
	$oldtime = time() - $option{maxtemp};
	foreach $name ($self->_volatile()) {
	    $path = $self->{path} . "/" . $name;
	    next unless _older($path, $oldtime);
	    warn("* removing too old volatile element: $name\n");
	    foreach $file (_directory_contents($path, 1)) {
		next if $file eq LOCKED_DIRECTORY;
		$fpath = "$path/$file";
		unlink($fpath) and next;
		_fatal("cannot unlink(%s): %s", $fpath, $!) unless $! == ENOENT;
	    }
	    _special_rmdir($path . "/" . LOCKED_DIRECTORY);
	    _special_rmdir($path);
	}
    }
    # iterate to find abandoned locked entries
    if ($option{maxlock}) {
	$oldtime = time() - $option{maxlock};
	$name = $self->first();
	while ($name) {
	    $path = $self->{path} . "/" . $name;
	    next unless $self->_state($name) eq STATE_LOCKED;
	    next unless _older($path, $oldtime);
	    warn("* removing too old locked element: $name\n");
	    $self->unlock($name, 1);
	} continue {
	    $name = $self->next();
	}
    }
}

1;

__END__

=head1 NAME

Directory::Queue - object oriented interface to a directory based queue

=head1 SYNOPSIS

  use Directory::Queue;

  #
  # simple schema:
  #  - there must be a "body" which is a string
  #  - there can be a "header" which is a table/hash
  #

  $schema = { "body" => "string", "header" => "table?" };
  $queuedir = "/tmp/test";

  #
  # sample producer
  #

  $dirq = Directory::Queue->new(path => $queuedir, schema => $schema);
  foreach $count (1 .. 100) {
      $name = $dirq->add(body => "element $count\n", header => \%ENV);
      printf("# added element %d as %s\n", $count, $name);
  }

  #
  # sample consumer
  #

  $dirq = Directory::Queue->new(path => $queuedir, schema => $schema);
  for ($name = $dirq->first(); $name; $name = $dirq->next()) {
      next unless $dirq->lock($name);
      printf("# reading element %s\n", $name);
      %data = $dirq->get($name);
      # one can use $data{body} and $data{header} here...
      # one could use $dirq->unlock($name) to only browse the queue...
      $dirq->remove($name);
  }

=head1 DESCRIPTION

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue. A Python implementation of the same algorithm is
available at L<http://code.google.com/p/dirq> so readers and writers
can even be written in different languages.

There is no knowledge of priority within a queue. If multiple
priorities are needed, multiple queues should be used.

=head1 TERMINOLOGY

An element is something that contains one or more pieces of data. A
simple string may be an element but more complex schemas can also be
used, see the L</SCHEMA> section for more information.

A queue is a "best effort FIFO" collection of elements.

It is very hard to guarantee pure FIFO behavior with multiple writers
using the same queue. Consider for instance:

=over

=item * Writer1: calls the add() method

=item * Writer2: calls the add() method

=item * Writer2: the add() method returns

=item * Writer1: the add() method returns

=back

Who should be first in the queue, Writer1 or Writer2?

For simplicity, this implementation provides only "best effort FIFO",
i.e. there is a very high probability that elements are processed in
FIFO order but this is not guaranteed. This is achieved by using a
high-resolution time function and having elements sorted by the time
the element's final directory gets created.

=head1 LOCKING

Adding an element is not a problem because the add() method is atomic.

In order to support multiple processes interacting with the same
queue, advisory locking is used. Processes should first lock an
element before working with it. In fact, the get() and remove()
methods report a fatal error if they are called on unlocked elements.

If the process that created the lock dies without unlocking the
element, we end up with a staled lock. The purge() method can be used
to remove these staled locks.

An element can basically be in only one of two states: locked or
unlocked.

A newly created element is unlocked as a writer usually does not need
to do anything more with the element once dropped in the queue.

Iterators return all the elements, regardless of their states.

There is no method to get an element state as this information is
usually useless since it may change at any time. Instead, programs
should directly try to lock elements to make sure they are indeed
locked.

=head1 CONSTRUCTOR

The new() method can be used to create a Directory::Queue object that
will later be used to interact with the queue. The following
attributes are supported:

=over

=item path

the queue toplevel directory (mandatory)

=item umask

the umask to use when creating files and directories
(default: use the running process' umask)

=item maxelts

the maximum number of elements that an intermediate directory can hold
(default: 16,000)

=item schema

the schema defining how to interpret user supplied data
(mandatory if elements are added or read)

=back

=head1 SCHEMA

The schema defines how user supplied data is stored in the queue. It
is only required by the add() and get() methods.

The schema must be a reference to a hash containing key/value pairs.

The key must contain only alphanumerical characters. It identifies the
piece of data and will be used as file name when storing the data
inside the element directory.

The value represents the type of the given piece of data. It can be:

=over

=item binary

the data is a sequence of binary bytes, it will be stored directly in
a plain file with no further encoding

=item string

the data is a text string (i.e. a sequence of characters), it will be
UTF-8 encoded

=item table

the data is a reference to a hash of text strings, it will be
serialized and UTF-8 encoded before being stored in a file

=back

By default, all pieces of data are mandatory. If you append a question
mark to the type, this piece of data will be marked as optional. See
the comments in the L</SYNOPSIS> section for more information.

=head1 METHODS

The following methods are available:

=over

=item new()

return a new Directory::Queue object (class method)

=item copy()

return a copy of the object; this can be useful to have independent
iterators on the same queue

=item path()

return the queue toplevel path

=item id()

return a unique identifier for the queue

=item count()

return the number of elements in the queue

=item first()

return the first element in the queue, resetting the iterator;
return an empty string if the queue is empty

=item next()

return the next element in the queue, incrementing the iterator;
return an empty string if there is no next element

=item add(DATA)

add the given data (a hash) to the queue and return the corresponding
element name; the schema must be known and the data must conform to it

=item lock(ELEMENT[, PERMISSIVE])

attempt to lock the given element and return true on success; if the
PERMISSIVE option is true (which is the default), it is not a fatal
error if the element cannot be locked and false is returned

=item unlock(ELEMENT[, PERMISSIVE])

attempt to unlock the given element and return true on success; if the
PERMISSIVE option is true (which is I<not> the default), it is not a
fatal error if the element cannot be unlocked and false is returned

=item remove(ELEMENT)

remove the given element (which must be locked) from the queue

=item get(ELEMENT)

get the data from the given element (which must be locked) and return
basically the same hash as what add() used; the schema must be known

=item purge([OPTIONS])

purge the queue by removing unused intermediate directories, removing
too old temporary elements and unlocking too old locked elements (aka
staled locks); note: this can take a long time on queues with many
elements; OPTIONS can be:

=over

=item maxtemp

maximum time for a temporary element (in seconds, default 300);
if set to 0, temporary elements will not be removed

=item maxlock

maximum time for a locked element (in seconds, default 600);
if set to 0, locked elements will not be unlocked

=back

=back

=head1 DIRECTORY STRUCTURE

All the directories holding the elements and all the files holding the
data pieces are located under the queue toplevel directory. This
directory can contain:

=over

=item temporary

the directory holding temporary elements, i.e. the elements being added

=item obsolete

the directory holding obsolete elements, i.e. the elements being removed

=item I<NNNNNNNN>

an intermediate directory holding elements; I<NNNNNNNN> is an 8-digits
long hexadecimal number

=back

In any of the above directories, an element is stored as a single
directory with a 14-digits long hexadecimal name I<SSSSSSSSMMMMMR> where:

=over

=item I<SSSSSSSS>

represents the number of seconds since the Epoch

=item I<MMMMM>

represents the microsecond part of the time since the Epoch

=item I<R>

is a random digit used to reduce name collisions

=back

Finally, inside an element directory, the different pieces of data are
stored into different files, named according to the schema. A locked
element contains in addition a directory named C<locked>.

=head1 SECURITY

There are no specific security mechanisms in this module.

The elements are stored as plain files and directories. The filesystem
security features (owner, group, permissions, ACLs...) should be used
to adequately protect the data.

By default, the process' umask is respected. See the class constructor
documentation if you want an other behavior.

If multiple readers and writers with different uids are expected, the
easiest solution is to have all the files and directories inside the
toplevel directory world-writable (i.e. umask=0). Then, the
permissions of the toplevel directory itself (e.g. group-writable) are
enough to control who can access the queue.

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright CERN 2010-2011
