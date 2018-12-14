.dat files are the HST measurements for CANDELS from Eric Bell
.in files are shell simulation catalogs from Robyn at 4 distances
hst_to_wfi.py reads in Eric's files and converts to 5 WFI filters and 3 potential blue filters (just interpolation and extrapolation) and produces the .txt files for background contamination (equivalent to do_M83-16)

make_mix.py uses wingtips to read in stars and galaxies files and combines into stips input


