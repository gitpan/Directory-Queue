use strict;
use warnings;
use ExtUtils::MakeMaker;

my(%param, $emv, $name);

%param = (
    NAME                => 'Directory::Queue',
    AUTHOR              => 'Lionel Cons <lionel.cons@cern.ch>',
    VERSION             => '1.1_3',
    ABSTRACT_FROM       => 'lib/Directory/Queue.pm',
    LICENSE             => 'perl',
    PL_FILES            => {},
    BUILD_REQUIRES => {
        'Encode'        => 0,
        'File::Temp'    => 0,
        'Test::More'    => 0,
    },
    PREREQ_PM => {
        'POSIX'         => 0,
        'Time::HiRes'   => 0,
        'UNIVERSAL'     => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz' },
    clean               => { FILES => 'Directory-Queue-*' },
);

($emv = $ExtUtils::MakeMaker::VERSION) =~ s/_//;
if ($emv < 6.5503) {
    # missing BUILD_REQUIRES
    foreach $name (keys(%{ $param{BUILD_REQUIRES} })) {
	$param{PREREQ_PM}{$name} = $param{BUILD_REQUIRES}{$name};
    }
    delete($param{BUILD_REQUIRES});
}
if ($emv < 6.31) {
    # missing LICENSE
    delete($param{LICENSE});
}

WriteMakefile(%param);
