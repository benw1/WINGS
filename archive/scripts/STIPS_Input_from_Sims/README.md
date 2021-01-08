# Combine point and extended source lists
Read in stellar catalogs from simulations with Absolute AB mags and 
background galaxy catlogs. Using the WingsTips lib, produce mixed list 
of objects including stars and appropriate sampling of background 
galaxies in STIPS input format

The make_mix.py scripts utilizes classes and methods defined in 
wingtips.py. Samples background galaxies at the same location 
in each filter to generate coherent images. 

The hst_to_wfi.py script is standalone, used to convert HST 
fluxes to WFI fluxes. 

The .dat files are the HST measurements for CANDELS from Eric Bell.

The .in files are shell simulation catalogs from Robyn Sanderson at 4 distances

The hst_to_wfi.py scrip reads in (for example) the .dat files and converts to 
5 WFI filter fluxess plus that for 3 potential blue filters through interpolation 
and extrapolation. It produces the ASCII files for background contamination 
(equivalent to what do_M83-16 does for point sources).

Next, make_mix.py uses "wingtips" to read in stars and galaxies files, and 
combines them to produce STIPS simulation inputs.
