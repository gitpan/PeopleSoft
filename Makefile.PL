use 5.008;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'		=> 'PeopleSoft',
    'VERSION_FROM'	=> 'lib/PeopleSoft/Tables.pm', 
    'PREREQ_PM'		=> {}, 
    ($] >= 5.005 ? 
     (ABSTRACT => 'PeopleSoft Utility Modules', 
      AUTHOR     => 'William Goedicke <goedicke@goedsole.com') : ()),
);
