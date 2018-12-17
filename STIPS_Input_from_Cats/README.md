do_M83-10.py reads in a pipeline-produced fits (gst or st) and outputs a text file

do_Comb_st5.py reads in the text files from above and outputs a file that is only what we need

do_Comb.py reads in all of the st5 files and combines into a single file, removing overlaps

do_M83-14.py  reads in the comb file and outputs ra, dec, and mags for 4 HST bands:  This includes using the PHAT data or blackbody assumption to fill in any missing measurements

do_M83-16.py reads in the output from 14 and converts mags to WFIRST bands and writes count rates in each band to separate file
