# Query Probable Stellar Color Between Filter Pairs

do_M31-3.py: Eeads in a "gst" file and outputs (if you comment out line 30) 
an ascii file with 3 columns (these are 3 colors of the stars in the gst table). 
It will run faster if it does not generate CMDs (lines 18,23, and 28)

do_M83-14.py: Uses the lookup table created by the first script when generating 
color conversions

do_colorTab1.py: Also produces a 3 column color conversion file, but it uses a blackbody
rather than real stars. Each line is for an assumed temperature.

do_ColorTab2.py: Same as previous but for AB mag colors (rather than Vega).


