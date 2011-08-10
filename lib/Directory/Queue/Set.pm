#+##############################################################################
#                                                                              #
# File: Directory/Queue/Set.pm                                                 #
#                                                                              #
# Description: object oriented interface to a set of Directory::Queue objects  #
#                                                                              #
#-##############################################################################

#
# module definition
#

package Directory::Queue::Set;
use strict;
use warnings;
our $VERSION  = "1.1_3";
our $REVISION = sprintf("%d.%02d", q$Revision: 1.4 $ =~ /(\d+)\.(\d+)/);

#
# used modules
#

use Directory::Queue;
use UNIVERSAL qw();

#
# object constructor
#

sub new : method {
    my($class, @list) = @_;
    my($self);

    $self = {};
    bless($self, $class);
    $self->add(@list);
    return($self);
}

#
# add one or more queues to the set
#

sub add : method {
    my($self, @list) = @_;
    my($dirq, $id);

    foreach $dirq (@list) {
	Directory::Queue::_fatal("not a Directory::Queue object: %s", $dirq)
	    unless UNIVERSAL::isa($dirq, "Directory::Queue");
	$id = $dirq->id();
	Directory::Queue::_fatal("duplicate queue in set: %s", $dirq->path())
	    if $self->{dirq}{$id};
	$self->{dirq}{$id} = $dirq->copy();
    }
    # reset the iterator
    delete($self->{elt});
}

#
# remove one or more queues from the set
#

sub remove : method {
    my($self, @list) = @_;
    my($dirq, $id);

    foreach $dirq (@list) {
	Directory::Queue::_fatal("not a Directory::Queue object: %s", $dirq)
	    unless UNIVERSAL::isa($dirq, "Directory::Queue");
	$id = $dirq->id();
	Directory::Queue::_fatal("missing queue in set: %s", $dirq->path())
	    unless $self->{dirq}{$id};
	delete($self->{dirq}{$id});
    }
    # reset the iterator
    delete($self->{elt});
}

#
# get the next element of the queue set
#

sub next : method {
    my($self) = @_;
    my($id, $name, $min_elt, $min_id);

    return() unless $self->{elt};
    foreach $id (keys(%{ $self->{elt} })) {
	$name = substr($self->{elt}{$id}, -14);
	next if defined($min_elt) and $min_elt le $name;
	$min_elt = $name;
	$min_id = $id;
    }
    unless ($min_id) {
	delete($self->{elt});
	return();
    }
    $min_elt = $self->{elt}{$min_id};
    $self->{elt}{$min_id} = $self->{dirq}{$min_id}->next();
    delete($self->{elt}{$min_id}) unless $self->{elt}{$min_id};
    return($self->{dirq}{$min_id}, $min_elt);
}

#
# get the first element of the queue set
#

sub first : method {
    my($self) = @_;
    my($id);

    return() unless $self->{dirq};
    delete($self->{elt});
    foreach $id (keys(%{ $self->{dirq} })) {
	$self->{elt}{$id} = $self->{dirq}{$id}->first();
	delete($self->{elt}{$id}) unless $self->{elt}{$id};
    }
    return($self->next());
}

#
# count the elements of the queue set
#

sub count : method {
    my($self) = @_;
    my($count, $id);

    return(0) unless $self->{dirq};
    $count = 0;
    foreach $id (keys(%{ $self->{dirq} })) {
	$count += $self->{dirq}{$id}->count();
    }
    return($count);
}

1;

__END__

=head1 NAME

Directory::Queue::Set - object oriented interface to a set of Directory::Queue objects

=head1 SYNOPSIS

  use Directory::Queue;
  use Directory::Queue::Set;

  $dq1 = Directory::Queue->new(path => "/tmp/q1");
  $dq2 = Directory::Queue->new(path => "/tmp/q2");
  $dqset = Directory::Queue::Set->new($dq1, $dq2);

  ($dq, $elt) = $dqset->first();
  while ($dq) {
      # you can now process the element $elt of queue $dq...
      ($dq, $elt) = $dqset->next();
  }

=head1 DESCRIPTION

This module can be used to put different queues into a set and browse
them as one queue. The elements from all queues are merged together
and sorted independently from the queue they belong to.

=head1 METHODS

The following methods are available:

=over

=item new([DIRQ...])

return a new Directory::Queue::Set object containing the given
Directory::Queue objects (class method)

=item add([DIRQ...])

add the given Directory::Queue objects to the queue set;
resetting the iterator

=item remove([DIRQ...])

remove the given Directory::Queue objects from the queue set;
resetting the iterator

=item first()

return the first (queue, element) couple in the queue set,
resetting the iterator;
return an empty list if the queue is empty

=item next()

return the next (queue, element) couple in the queue set; return an
empty list if there is no next element

=item count()

return the total number of elements in all the queues of the set

=back

=head1 AUTHOR

Lionel Cons L<http://cern.ch/lionel.cons>

Copyright CERN 2010
