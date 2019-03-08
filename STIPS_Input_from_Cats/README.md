# Real Stellar Catalogs to Simulated Images in Sythetic Bands

do_M83-10.py: Reads in a DOLPHOT produced fits (gst or st) file 
and outputs a corresponding ASCII file

do_Comb_st5.py: Reads in the ASCII files produced by the previous 
code and outputs a file that is only what the following script  needs

do_Comb.py: Reads in all of the st5 files produced by the previous 
script and combines into a single file, removing overlapping regions 
in the catalogs. This critical, since otherwise any resulting simulated 
image will end up with stripes of overlapping stars.

do_M83-14.py: Reads in the comb file produced by the previous script 
and outputs RA, DEC, and magnitudes for 4 HST bands. This step includes 
using the stellar or blackbody color extrapolation to fill in any missing 
measurements

do_M83-16.py: Reads in the output from the previous script and converts mags 
to detector counts for WFIRST bands and writes count rates in each band to 
separate files that can be then used to generate STIPS inputs.
