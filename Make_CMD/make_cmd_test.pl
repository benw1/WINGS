#!/usr/bin/perl

#system "eval `/astro/users/benw1/.ureka/ur_setup -sh` \n ls *.fits | xargs ./make_cmd.py > log_cmd";

system "ls *.fits | xargs ./make_cmd.py > log_cmd";

exit 0;
