do_M31-3.py  reads in a PHAT gst file and outputs (if you comment out line 30) an ascii file with 3 columns (these are 3 colors of the stars in the gst table)

It will run faster if it does not generate CMDs (lines 18,23, and 28)

do_M83-14.py uses this lookup table when generating color conversions

do_colorTab1.py also produces a 3 column color file, but it uses a blackbody.  Each line is for an assumed temperature

do_ColorTab2.py Same as 1 but for AB mag colors


