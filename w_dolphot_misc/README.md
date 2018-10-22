# CleanPhotometry
The clean_photometry.py script uses supervised classification techniques to 
identify stars from noisy astronomical catalogs containing stars,
galaxies and noise from many different sources. With all 
dependencies installed (python3, scikit-learn, Pandas, NumPy, SciPy, AstroPy,
Matplotlib, graphviz etc.) the simplest use case is:

```
./clean_photomtry.py $path/filename.phot
```

where filename.phot is the raw DOLPHOT photomtery output. Please see the 
IPython notebooks Quick_Demo and Code_Details for details on the functions and
parameters as well as the required and optional input files.

# Background
This tool is built as part of the WFIRST simulations, analysis and 
recommendation pipeline under development at the University for Washington 
primarily for carrying out Nearby Galaxies projects. However, its 
functionalities are broadly reusable for a varity of tasks, as summarized 
toward the bottom of this page.

Prior implementation of this tool (Cull_Photometry.py) has similar top level 
capability, but was implemented as an iterative  quality parameter outlier 
removal tool. While highly effective, that implementation proved both 
computationally inefficient less adoptable for general use. Howevever, it 
serves as an useful baseline estinator and validator for this implementation.

# The Recommendation Pipeline

In practice, on the order of 100,000 sets (18 detectors on the telescope, 
various distance of galaxies, many galaxy morphologies etc.) of simulations 
will be run on a hybrid system of HPC facilities and cloud resources, to 
generate a reference library for the recommendation system. The pipeline 
backbone is currently being prototyped. 
  
Key steps and components of the WFIRST pipeline are outluned below. This module
primarily concern item 4 below.

1. Generate input source list
 - Dynamical galaxy simulation and stellar evolution models 
   generate point source lists
 - Hubble Deep Field background galaxy profiles and photometry 
   interpolated to WFIRST filters to generate realistic background
   source lists
 - Stars and galaxy lists combined appropriately and converted 
   to STIPS ingestable synthetic input source catalog

2. The Space Telescope Image Products Simulator (STIPS) produces 
   simulated WFIRST Wide Field Images (WFI) images

3. DOLPHOT's WFI package produces photometric measurements of all 
   plausible sources using a pre-generated Point Spread Function 
   libraries for each filter produced using WebbPSF

4. The synthetic input source list and measured output photometry 
   are anylsed to evaluate effectiveness of the observation strategy 
   (this module; please see below for details)

5. Recommend optimal resource allocation (integration times, filters)
   to maximize science return

# Photometry Cleaning Workflow

1. Read in the files ( read_data() )
 - Input: sythetic photometry file for image generation, IPAC format
 - Output: DOLPHOT measured raw photometry, ASCII format

2. Prepare the data for classification ( prep_data() )
 - Clean and validate output data ( validate_output() )
  - Remove measurements with unphysical values, such as negative countrate
  - Remove least informative entries, such as magnitude errors >0.5 (log scale)
  - Remove missing value indicators such as +/- 9.99
 - Create Pandas Dataframes for input source lists and output source list 
   including quality parameters ( pack_input(), pack_output() )
 - Label output data entries ( label_output() )
  - Match each remaining output entry with the closest valid (<valid_mag)
    input entry within matching radius specified by 'tol'. Those matched 
    to point source input are labeled '1', everything else get '0' label.
    Optionally: use sky_soordinates from the simulated images since the 
    images may not be aligned to each other. [(match_in_out(), match_lists(),
    match_cats() etc.)

3. Initiate a classifier and train/test/evaluate the model for each filter 
   for the selected features ( classify() )
 - Features selected out of the box, based on domain knowledge to likely 
   use cases and to avoid over-cleaning of data
 - Inititate, e.g., DecisionTreeClassifier(max_depth=4, min_samples_split=50,
                                            min_samples_leaf=10)
  - Optimized parameters. Greater depth does not help
  - RandomForest and AdaBoost did not imporve performance
 - Do train/test split for specified test_size, fit the model and predict 
   labels for "test" dataset.
 - Evaluate the classification model
   - Score the classifier for both classes and each class separately.
   - Manually calculate Precision, Recall, Specficity etc. as sanity check.
   - Determine feature importances
 - Produce new labels for *all* output entries, both train and test. This is 
   to enable the next step, which examines practical implications of using the 
   model.

4. Produce figures and text to demonstrate practicality of the model for the
   intended use case ( makePlots(), print_report() etc.  )
 - Consider each possible pairs of filters, since all data will be available
   in at least two filters. Keep only sources originally added to both filter 
   above valid_mag threshold and those classified at both filters as point 
   sources. ( input_pair(), output_pair() )
 - Match the two pairs against each other to ensure validity. Mark sources 
   added and classified at both filters as point sources to be 'Stars', and 
   those classfied at both filters as point sources but not added likewise 
   as 'Others'.

5. Draw qualititative conclusions from the figures and text reports, such as:
 - Which filter pairs perform best at correctly classifying stars?
 - Among pairs with comparable performance, which would also be of most 
   utility to scientists using the telescope?
 - Which features are most influential? What is the science implication 
   of the that?
 - Do the Decision Tree representation appear reasonable and/or consistent 
   with conventionally accepted approaches?
   
# Proposed Enhancements / Generalization
Since for real astronomical data there are no input source lists, and 
unsupervised classification schemes can become noise dominated in this 
context, a semi-supervised recursive classfier can be implemented. For example,
a conservative implementation of the old 'outlier removal' implementation 
can be used to create custom train/test datasets. The "stars" class can then 
be gradually broadened up to some predefined stooping criteria.

A more general enhancement would be classifying based on features in multiple
filters, However, two distinct complications need to be resolved. First, 
even with feature scaling, the distribution of the various quality 
parameters are and the information they encapsulate are very different and 
often causally related. Second, same features in different filters are by 
definition highly correlated. As such, multi-filter classification asymptotes 
to the outliers removal solution, albeit faster.

