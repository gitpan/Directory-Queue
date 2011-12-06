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
our $VERSION  = "1.4";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.37 $ =~ /(\d+)\.(\d+)/);

#
# used modules
#

use Directory::Queue::Base qw(:DIR :FILE :RE :ST _fatal _name);
use POSIX qw(:errno_h);

#
# inheritance
#

our(@ISA) = qw(Directory::Queue::Base);

#
# constants
#

# name of the directory holding temporary elements
use constant TEMPORARY_DIRECTORY => "temporary";

# name of the directory holding obsolete elements
use constant OBSOLETE_DIRECTORY => "obsolete";

# name of the directory indicating a locked element
use constant LOCKED_DIRECTORY => "locked";

#
# global variables
#

our(
    $_FileRegexp,	  # regexp matching a file in an element directory
    %_Byte2Esc,           # byte to escape map
    %_Esc2Byte,           # escape to byte map
);

$_FileRegexp = qr/[0-9a-zA-Z]+/;
%_Byte2Esc   = ("\x5c" => "\\\\", "\x09" => "\\t", "\x0a" => "\\n");
%_Esc2Byte   = reverse(%_Byte2Esc);

#+++############################################################################
#                                                                              #
# Helper Functions                                                             #
#                                                                              #
#---############################################################################

#
# transform a hash of strings into a string (reference)
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
    return(\$string);
}

#
# transform a string (reference) into a hash of strings
#
# note:
#  - duplicate keys are not checked (the last one wins)
#

sub _string2hash ($) {
    my($stringref) = @_;
    my($line, $key, $value, %hash);

    foreach $line (split(/\x0a/, $$stringref)) {
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

# stat version (faster):
#  - lstat() is used so symlinks are not followed
#  - this only checks the number of hard links
#  - we do not even check that the given path indeed points to a directory!

sub _subdirs_stat ($) {
    my($path) = @_;
    my(@stat);

    @stat = lstat($path);
    unless (@stat) {
	_fatal("cannot lstat(%s): %s", $path, $!) unless $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return();
    }
    return($stat[ST_NLINK] - 2);
}

# readdir version (slower):
#  - we really count the number of entries
#  - we however do not check that these entries are themselves indeed directories

sub _subdirs_readdir ($) {
    my($path) = @_;

    return(scalar(_special_getdir($path)));
}

# use the right version (we cannot rely on hard links on DOS-like systems)

if ($^O =~ /^(cygwin|dos|MSWin32)$/) {
    *_subdirs = \&_subdirs_readdir;
} else {
    *_subdirs = \&_subdirs_stat;
}

#
# check the given string to make sure it represents a valid element name
#

sub _check_element ($) {
    my($element) = @_;

    _fatal("invalid element: %s", $element)
	unless $element =~ m/^(?:$_DirectoryRegexp)\/(?:$_ElementRegexp)$/o;
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
    my($self, $name, $path, $options);

    # default object
    $self = __PACKAGE__->SUPER::new(%option);
    foreach $name (qw(path umask)) {
	delete($option{$name});
    }
    # default options
    $self->{maxelts} = 16_000;    # maximum number of elements allowed per directory
    # check maxelts
    if (defined($option{maxelts})) {
	_fatal("invalid maxelts: %s", $option{maxelts})
	    unless $option{maxelts} =~ /^\d+$/ and $option{maxelts} > 0;
	$self->{maxelts} = delete($option{maxelts});
    }
    # check schema
    if (defined($option{schema})) {
	_fatal("invalid schema: %s", $option{schema})
	    unless ref($option{schema}) eq "HASH";
	foreach $name (keys(%{ $option{schema} })) {
	    _fatal("invalid schema name: %s", $name)
		unless $name =~ /^($_FileRegexp)$/ and $name ne LOCKED_DIRECTORY;
	    _fatal("invalid schema type: %s", $option{schema}{$name})
		unless $option{schema}{$name} =~ /^(binary|string|table)([\?\*]{0,2})$/;
	    $self->{type}{$name} = $1;
	    $options = $2;
	    $self->{mandatory}{$name} = 1 unless $options =~ /\?/;
	    $self->{ref}{$name} = 1 if $options =~ /\*/;
	    _fatal("invalid schema type: %s", $option{schema}{$name})
		if $self->{type}{$name} eq "table" and $self->{ref}{$name};
	}
	_fatal("invalid schema: no mandatory data")
	    unless $self->{mandatory};
	delete($option{schema});
    }
    # check unexpected options
    foreach $name (keys(%option)) {
	_fatal("unexpected option: %s", $name);
    }
    # create directories
    foreach $name (TEMPORARY_DIRECTORY, OBSOLETE_DIRECTORY) {
	$path = $self->{path} . "/" . $name;
	_special_mkdir($path, $self->{umask}) unless -d $path;
    }
    # so far so good...
    return($self);
}

#
# return the number of elements in the queue, regardless of their state
#

sub count : method {
    my($self) = @_;
    my($count, $name, @list, $subdirs);

    $count = 0;
    # get the list of existing directories
    foreach $name (_special_getdir($self->{path}, "strict")) {
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
# check if an element is locked:
#  - this is best effort only as it may change while we test (only locking is atomic)
#  - if given a time, only return true on locks older than this time (needed by purge)
#

# version using nlink (faster)

sub _is_locked_nlink : method {
    my($self, $name, $time) = @_;
    my($path, @stat);

    $path = $self->{path} . "/" . $name;
    @stat = lstat($path);
    unless (@stat) {
	_fatal("cannot lstat(%s): %s", $path, $!) unless $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return(0);
    }
    # locking increases nlink so...
    return(0) unless $stat[ST_NLINK] > 2;
    # check age if time is given
    return(0) if defined($time) and $stat[ST_MTIME] >= $time;
    # so far so good but we double check that the proper directory does exist
    return(-d $path . "/" . LOCKED_DIRECTORY);
}

# version not using nlink (slower)

sub _is_locked_nonlink : method {
    my($self, $name, $time) = @_;
    my($path, @stat);

    $path = $self->{path} . "/" . $name;
    return(0) unless -d $path . "/" . LOCKED_DIRECTORY;
    return(1) unless defined($time);
    @stat = lstat($path);
    unless (@stat) {
	_fatal("cannot lstat(%s): %s", $path, $!) unless $! == ENOENT;
	# RACE: this path does not exist (anymore)
	return(0);
    }
    return($stat[ST_MTIME] < $time);
}

# use the right version (we cannot rely on hard links on DOS-like systems)

if ($^O =~ /^(cygwin|dos|MSWin32)$/) {
    *_is_locked = \&_is_locked_nonlink;
} else {
    *_is_locked = \&_is_locked_nlink;
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
	    # (this can happen if an other process locked & removed the element
	    #  while our mkdir() was in progress... yes, this can happen!)
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
    _fatal("cannot remove %s: not locked", $element) unless $self->_is_locked($element);
    # move the element out of its intermediate directory
    $path = $self->{path} . "/" . $element;
    while (1) {
	$temp = $self->{path} . "/" . OBSOLETE_DIRECTORY . "/" . _name();
	rename($path, $temp) and last;
	_fatal("cannot rename(%s, %s): %s", $path, $temp, $!)
	    unless $! == ENOTEMPTY or $! == EEXIST;
	# RACE: the target directory was already present...
    }
    # remove the data files
    foreach $name (_special_getdir($temp, "strict")) {
	next if $name eq LOCKED_DIRECTORY;
	_fatal("unexpected file in %s: %s", $temp, $name)
	    unless $name =~ /^($_FileRegexp)$/o;
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
	# while it was being removed (see the comment in the lock() method)
	# so we try to remove the lock again and again...
    }
}

#
# get an element from a locked element
#

sub get : method {
    my($self, $element) = @_;
    my(%data, $name, $path, $ref);

    _fatal("unknown schema") unless $self->{type};
    _check_element($element);
    _fatal("cannot get %s: not locked", $element) unless $self->_is_locked($element);
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
	if ($self->{type}{$name} =~ /^(binary|string)$/) {
	    $ref = _file_read($path, $self->{type}{$name} eq "string");
	    $data{$name} = $self->{ref}{$name} ? $ref : $$ref;
	} elsif ($self->{type}{$name} eq "table") {
	    $data{$name} = _string2hash(_file_read($path, 1));
	} else {
	    _fatal("unexpected data type: %s", $self->{type}{$name});
	}
    }
    return(\%data) unless wantarray();
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
    foreach $name (_special_getdir($self->{path}, "strict")) {
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
    my($self, @data) = @_;
    my($data, $temp, $dir, $name, $path, $ref, $utf8);

    _fatal("unknown schema") unless $self->{type};
    if (@data == 1) {
	$data = $data[0];
    } else {
	$data = { @data };
    }
    while (1) {
	$temp = $self->{path} . "/" . TEMPORARY_DIRECTORY . "/" . _name();
	last if _special_mkdir($temp, $self->{umask});
    }
    foreach $name (keys(%$data)) {
	_fatal("unexpected data: %s", $name) unless $self->{type}{$name};
	if ($self->{type}{$name} =~ /^(binary|string)$/) {
	    if ($self->{ref}{$name}) {
		_fatal("unexpected %s data in %s: %s",
		       $self->{type}{$name}, $name, $data->{$name})
		    unless ref($data->{$name}) eq "SCALAR";
		$ref = $data->{$name};
	    } else {
		_fatal("unexpected %s data in %s: %s",
		       $self->{type}{$name}, $name, $data->{$name})
		    if ref($data->{$name});
		$ref = \$data->{$name};
	    }
	    $utf8 = $self->{type}{$name} eq "string";
	} elsif ($self->{type}{$name} eq "table") {
	    _fatal("unexpected %s data in %s: %s", $self->{type}{$name}, $name, $data->{$name})
		unless ref($data->{$name}) eq "HASH";
	    $ref = _hash2string($data->{$name});
	    $utf8 = 1;
	} else {
	    _fatal("unexpected data type in %s: %s", $name, $self->{type}{$name});
	}
	eval {
	    _file_write("$temp/$name", $utf8, $self->{umask}, $ref);
	};
	if ($@) {
	    if ($@ =~ /^Wide character in /) {
		_fatal("unexpected wide character in %s: %s", $name, $data->{$name});
	    } else {
		die($@);
	    }
	}
    }
    foreach $name (keys(%{ $self->{mandatory} })) {
	_fatal("missing mandatory data: %s", $name)
	    unless defined($data->{$name});
    }
    $dir = $self->_insertion_directory();
    while (1) {
	$name = $dir . "/" . _name();
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

    foreach $name (_special_getdir($self->{path} . "/" . TEMPORARY_DIRECTORY)) {
	push(@list, TEMPORARY_DIRECTORY . "/" . $1)
	    if $name =~ /^($_ElementRegexp)$/o; # untaint
    }
    foreach $name (_special_getdir($self->{path} . "/" . OBSOLETE_DIRECTORY)) {
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
    foreach $name (_special_getdir($self->{path}, "strict")) {
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
	    foreach $file (_special_getdir($path)) {
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
	    next unless $self->_is_locked($name, $oldtime);
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
  # sample consumer (one pass only)
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

  #
  # looping consumer (sleeping to avoid using all CPU time)
  #

  $dirq = Directory::Queue->new(path => $queuedir, schema => $schema);
  while (1) {
      sleep(1) unless $dirq->count();
      for ($name = $dirq->first(); $name; $name = $dirq->next()) {
          ... same as above ...
      }
  }

=head1 DESCRIPTION

The goal of this module is to offer a simple queue system using the
underlying filesystem for storage, security and to prevent race
conditions via atomic operations. It focuses on simplicity, robustness
and scalability.

This module allows multiple concurrent readers and writers to interact
with the same queue. A Python implementation of the same algorithm is
available at http://pypi.python.org/pypi/dirq/ so readers and writers
can be written in different programming languages.

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
high-resolution timer and having elements sorted by the time their
final directory gets created.

=head1 LOCKING

Adding an element is not a problem because the add() method is atomic.

In order to support multiple reader processes interacting with the
same queue, advisory locking is used. Processes should first lock an
element before working with it. In fact, the get() and remove()
methods report a fatal error if they are called on unlocked elements.

If the process that created the lock dies without unlocking the
element, we end up with a staled lock. The purge() method can be used
to remove these staled locks.

An element can basically be in only one of two states: locked or
unlocked.

A newly created element is unlocked as a writer usually does not need
to do anything more with it.

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

the data is a binary string (i.e. a sequence of bytes), it will be
stored directly in a plain file with no further encoding

=item string

the data is a text string (i.e. a sequence of characters), it will be
UTF-8 encoded before being stored in a file

=item table

the data is a reference to a hash of text strings, it will be
serialized and UTF-8 encoded before being stored in a file

=back

By default, all pieces of data are mandatory. If you append a question
mark to the type, this piece of data will be marked as optional. See
the comments in the L</SYNOPSIS> section for an example.

By default, string or binary data is used directly. If you append an
asterisk to the type, the data that you add or get will be by
reference. This can be useful to avoid string copies of large amounts
of data.

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

add the given data (a hash or hash reference) to the queue and return
the corresponding element name; the schema must be known and the data
must conform to it

=item lock(ELEMENT[, PERMISSIVE])

attempt to lock the given element and return true on success; if the
PERMISSIVE option is true (which is the default), it is not a fatal
error if the element cannot be locked and false is returned

=item unlock(ELEMENT[, PERMISSIVE])

attempt to unlock the given element and return true on success; if the
PERMISSIVE option is true (which is I<not> the default), it is not a
fatal error if the element cannot be unlocked and false is returned

=item touch(ELEMENT)

update the access and modification times on the element's directory to
indicate that it is still being used; this is useful for elements that
are locked for long periods of time (see the purge() method)

=item remove(ELEMENT)

remove the given element (which must be locked) from the queue

=item get(ELEMENT)

get the data from the given element (which must be locked) and return
basically the same hash as what add() got (in list context, the hash
is returned directly while in scalar context, the hash reference is
returned instead); the schema must be knownand the data must conform
to it

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
