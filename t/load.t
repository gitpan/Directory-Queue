#!perl

use Test::More tests => 1;

BEGIN {
    use_ok( 'Directory::Queue' ) || print "Bail out!\n";
}

#diag( "Testing Directory::Queue $Directory::Queue::VERSION, Perl $], $^X" );
