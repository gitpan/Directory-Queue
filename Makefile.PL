use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Directory::Queue',
    AUTHOR              => q{Lionel Cons <lionel.cons@cern.ch>},
    VERSION             => '0.5',
    ABSTRACT_FROM       => 'lib/Directory/Queue.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'POSIX'         => 0,
        'Time::HiRes'   => 0,
        'Test::More'    => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Directory-Queue-*' },
);
