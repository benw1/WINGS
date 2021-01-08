#!/usr/bin/perl

#
# make_cmd.pl
#
# Copyright University of Washington, 2007
# Authors:
#    Ben Williams        ben@astro.washington.edu
#    Keith Rosema        krosema@astro.washington.edu
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#
# This is a perl task that is part of the acs reduction pipeline.
# It's job is to make a color-magnitude diagram using the binary
# fits photometry file.
#
#
# This task is meant to be invoked in a target/proc subdirectory -
# Configuration information is obtained from the target configuration
# utilites, which reference data stored in a configuration database.
#
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

#
# Installation note: Pipeline perl directories should be installed
#                    in a location accessible to the perl library
#                    modules
#                    on Robert, this location is
#                    /share/bigdisk1/PROJECTS/APPS/lib/perl/
#                    on Kix, this location is
#                    /astro/apps/angst/lib/perl
use Getopt::Std;
use acs2;
# --------------
# Initialization
# --------------

sub pipeline_register {
    my $progname = shift;
    my $config = acs2::jpipecore::PipelineConfig->new();
    $config->validate();
    my $pipeline = $config->pipelineFetch();
    
    my $task = $pipeline->registerTask($progname);
    
    $task->addMask("*","start",$task->getName());
    $task->addMask("*","culling_done","*");

    $task->addMask($task->getName(),"failed","1");

}

$globallog = undef;
$logfile = undef;

sub getLogFile {
  my ($logname, $configuration) = @_;
  my $logfile = undef;
  
  unless (defined($logname)) {
    $logname  = $configuration->getParameterStringValue('logfile');
    $logname  = "sort.log" unless defined ($logname);
  }
  $logfile = $configuration->getPathFor($logname, "log");
  return $logfile;
}

sub openLog {
    my $logpath = shift;
    open LOG, ">> $logpath" or die "tag_image.pl unable to open $logpath!\n";
    $globallog = \*LOG;
    return $globallog;
}

sub logprint {
    my @logargs = @_;
    if (defined($globallog)) {
        print $globallog @logargs;
    } else {
        print @logargs;
    }
}
sub closeLog {
    close $globallog if defined($globallog);
}



%options=();   # Import command line options
getopts("tni:E:J:Rh",\%options);

if (defined($options{h})) {
    print "NAME:                                                     \n";
    print "  make_cmd.pl  - makes a color-magnitude diagram          \n";
    print "                 using the binary fits table of           \n";
    print "                 photometry.                              \n";
    print "SYNOPSIS                                                  \n";
    print "  make_cmd.pl [options]                             \n";
    print "                                                          \n";
    print "DESCRIPTION                                               \n";
    print "  This is a perl task that is part of the acs reduction   \n";
    print "  pipeline. It's job is to make a color_magnitude diagram \n";
    print "  from the binary fits table using IDL.                   \n";
    print "                                                          \n";
    print "  This task is meant to be invoked by the main pipeline   \n";
    print "  script. Configuration information is obtained from the  \n";
    print "  pipeline configuration utilites, which reference data   \n";
    print "  stored in a configuration database.                     \n";
    print "                                                          \n";
    print "OPTIONS                                                   \n";
    print "  -t    run on the specific target name provided          \n";
    print "                                                          \n";
    print "  -n    The configuration will be specified by the name   \n";
    print "        of a configuration file. (default assumption)     \n";
    print "                                                          \n";
    print "  -i    The configuration will be specified by id,        \n";
    print "        (the database primary key).                       \n";
    print "                                                          \n";
    print "  -E eventid                                              \n";
    print "        When registering with the pipeline, this task may \n";
    print "        request that it be informed about the event that  \n";
    print "        brought it to life.  The id of the source event   \n";
    print "        can be passed by the pipeline to this program     \n";
    print "        through the -E parameter.                         \n";
    print "                                                          \n";
    print "  -J jobid                                                \n";
    print "        When registering with the pipeline, this task may \n";
    print "        request that it be informed about the job that    \n";
    print "        spawned it.                                       \n";
    print "                                                          \n";
    print "                                                          \n";
    print "  -R    Run in registration mode.  Causes this program    \n";
    print "        to invoke its pipeline registration subroutine,   \n";
    print "        then exit.  This only needs to be done once when  \n";
    print "        new or modified code is inserted into the         \n";
    print "        Pipeline system.  Usually this is done            \n";
    print "        automatically by running update_pipeline.pl i     \n";
    print "                                                          \n";
    print "  -h    Print this help message.                          \n";
    print "                                                          \n";
    print "                                                          \n";
    die 1;
}


if (defined ($options{R})) {             # special run case
    &pipeline_register($0, $options{p}); # just register with the pipeline
    exit(0);
}
##Get Target and configuration information
my $lastprogram = 0;
my $event;
my $previous_job;
my $thisjob;
my $this_configuration;
my $pipeline;
my $this_target;


unless (defined($options{J}) && defined($options{E})) {
  printf "We require the -J job and -E event options to be specified\n";
  exit(-1);
}
my $config = acs2::jpipecore::PipelineConfig->new();   # config is the _pipeline_ configuration, not _job_ configuration
   $config->validate();
my $pipeline = $config->pipelineFetch();

print "Config = ",$config,"\n";

print "Event = ",$options{E},"\n";
my $event = acs2::jpipecore::Event->retrieve($options{E});
die "make_cmd.pl couldn't find its initial event!\n" unless defined($event);
$event->dump("");
print "Event = ",$event,"\n";
my $thisjob = acs2::jpipecore::Job->getThis($options{J});
print "JOB ",$thisjob->getId(),"\n";
my $previous_configuration = $thisjob->getConfiguration();
$tid  = $event->getOptionValue("target_id");       
print "TARGET ID = $tid\n";
$this_target = acs2::jpipecore::Target->retrieve($tid);
print "TARGET NAME = ",$this_target->getName(),"\n";
#$this_configuration = $thisjob->getConfiguration();
$this_configuration   =  $this_target->getNamedConfiguration($previous_configuration->getName());
#$this_target = $this_configuration->getTarget();
#print "TARGET NAME = ",$this_target->getName(),"\n";

#initialize the log file for this step
$logname = "make_cmd.e" . $event->getId() . ".j" . $thisjob->getId() ;
my $logfile = &getLogFile($logname,$this_configuration);              
openLog($logfile);
&logprint("\n\nMAKE_CMD STARTED!\n\n");
$lastprogram = $event->getJobName();
$previous_job = $event->getSourceJob();

my $default_configuration = $this_target->getNamedConfiguration("default");
&logprint("THIS CONFIGURATION ID IS ",$this_configuration->getId(),"\n");
&logprint("DEFAULT CONFIGURATION ID IS ",$default_configuration->getId(),"\n");

# Identify the configuration by number or name
unless (defined ($this_configuration)){
    if (defined ($options{i})) {
        if (defined($this_configuration)) {
            die "-i was used to specify a Configuration, but there already is one!\n";
        }
        #otherwise get the target from the configuration it was handed
        $configid = $options{i};
        $this_configuration = $this_target->getConfiguration($configid);
    } elsif (defined($options{n})) {
        $configid = $options{n};
        $this_configuration = $this_target->getNamedConfiguration($configid);
    } else {
        warn "No configuration specified... using $config_path \n";
    }
}

if (!defined($this_configuration)) {
    die "There is no configuration for $configid \n",
    "Have you considered using configure.pl?\n";
}

unless (defined ($this_target)){
    &logprint("calling getTarget on this_configuration \n");
    $this_target = $this_configuration->getTarget();
}
#

if (defined($thisjob)) {
    $thisjob->dump("");
} 

#Now grab the pipeline;
my $pipeline = $this_configuration->getTarget()->getPipeline();
$pipeline->dump("");


$detector = "WFC-UVIS-IR";

$gal = $this_target->getName();

undef @files;
$files[++$#files] = $gal . ".st.fits";
$files[++$#files] = $gal . ".gst.fits";

$filters1 = acs2::jpipecore::DataProduct->getUniqueFilters($this_configuration, 'proc');
undef @filters;

foreach  ($f=0; $f < $filters1->length;  $f++){
    $filt = $filters1->[$f];
    $filt =~ s/\ //;
    $filt =~ s/\ //;
    $filters[++$#filters] = $filt;
}

foreach ($i=0;$i<$#filters+1;$i++){
    $filters[$i] =~ s/F1/F9/;
}
@filters = sort(@filters);

foreach ($i=0;$i<$#filters+1;$i++){
    $filters[$i] =~ s/F9/F1/;
}
  

$pipeline->addFile($this_configuration->getFileFor(@files[0],'proc'));
$pipeline->addFile($this_configuration->getFileFor(@files[1],'proc'));
$pipeline->s3commit();


foreach  $filter (@filters){
     $filtnames = $filtnames . $filter . ",";
}
chop $filtnames;


foreach $line1 (@files){

    $line = $this_configuration->getPathFor($line1,'proc');

    &logprint("FITS file is $line\n");

    &logprint("No Photometry Fits File found! Exiting...\n") unless -e $line;
    die "No Photometry Fits File found! Exiting...\n" unless -e $line;
    
    $line2 = $this_configuration->getPathFor($line1,'conf');
    
    $pro = $line2 . "_cmd.pro";
    
    ($pid,$targname) = split '\_', $gal;
    $gridbreak=4;
    if ($detector =~ /WFC/ || $detector =~ /HRC/){
	if ($detector =~ /WFC/){
	    $gridbreak = 8;
	}
	$detector = "ACS";
    }
    if ($detector =~ /UVIS/){
	$gridbreak = 8;
    }
    
    $clevels = $this_configuration->getParameter("clevels")->getValue();
    $clevels =~ s/\"//;
    $clevels =~ s/\"//;
    $threshold = $this_configuration->getParameter("threshold")->getValue();
    $gridsize = $this_configuration->getParameter("gridsize")->getValue();
    $galaxy1 =  $this_configuration->getParameter("Galaxy");
    if (defined($galaxy1)){ $galaxy = $galaxy1->getValue();}
    unless(defined($galaxy1)){
      $galaxy = "M31";
      $this_configuration->addParameter("Galaxy",$galaxy);
      }
    $mag1 = $this_configuration->getParameter("mlimlow")->getValue();
    $mag2 = $this_configuration->getParameter("mlimhi")->getValue();
    $col1 = $this_configuration->getParameter("climlow")->getValue();
    $col2 = $this_configuration->getParameter("climhi")->getValue();
    
    $cmderr = $pipeline->getSwBuildDir() . "/pipe\_3.0/contcmd_err.pro";
    $cmderrgrid = $pipeline->getSwBuildDir() . "/pipe\_3.0/contcmd_err_grid.pro";
    
    &logprint("IDL Script is $pro\n");
    open PRO, ">$pro";
    
    print PRO "infile1=\'$line\'\n";

    foreach ($i=0;$i<$filters1->length-1;$i++){
	foreach ($j=$i+1;$j<$filters1->length;$j++){    
	    $filt1 = @filters[$i];    
	    $filt2 = @filters[$j];
	    $outfile = $gal . "_" . $filt1 . "_". $filt2 . ".gst_cmd.ps" if $line =~ /\.gst/;
	    $outfile2 = $gal . "_" . $filt1 . "_". $filt2 . ".gst_gridcmd.ps" if $line =~ /\.gst/;
	    $outfile = $gal . "_" . $filt1 . "_". $filt2 . ".st_cmd.ps" unless $line =~ /\.gst/;
	    $outfile2 = $gal . "_" . $filt1 . "_". $filt2 . ".st_gridcmd.ps" unless $line =~ /\.gst/;
	    $file = $this_configuration->getFileFor("$outfile","proc");   
	    $file2 = $this_configuration->getFileFor("$outfile2","proc");   
	    $outfile = $this_configuration->getPathFor("$outfile","proc");
	    $outfile2 = $this_configuration->getPathFor("$outfile2","proc");
	    $outdp = $file->getDataProduct();
	    $outdp2 = $file2->getDataProduct();

	    print PRO ".r $cmderr\n";
	    print PRO ".r $cmderrgrid\n";
	    print PRO "outfile=\'$outfile\'\n";
	    print PRO "columns = ['$filt1\_vega\', '$filt2\_vega\','$filt1\_err', '$filt2\_err','X','Y']\n";
	    print PRO "data = mrdfits(infile1,1,columns=columns)\n";
	    print PRO "mag1 = data.$filt1\_vega\n";
	    print PRO "mag2 = data.$filt2\_vega\n";
	    print PRO "magerr1 = data.$filt1\_err\n";
	    print PRO "magerr2 = data.$filt2\_err\n";
	    print PRO "xpix = data.x\n";
	    print PRO "ypix = data.y\n";
	    print PRO "contcmd_err,OUTFILE=outfile,MAG1=mag1,MAG2=mag2,MAGERR1=magerr1,MAGERR2=magerr2,BINSIZE=$gridsize,XMIN=$col1,YMIN=$mag1,XMAX=$col2,YMAX=$mag2,LEVELS=\[$clevels\],THRESHOLD=$threshold,XLABEL='$filt1-$filt2',YLABEL='$filt2',PLOTTITLE='$galaxy',PID='PID: $pid',TARG='$targname'\n";
	    print PRO "data = mrdfits(infile1,1,columns=columns)\n";
	    print PRO "mag1 = data.$filt1\_vega\n";
	    print PRO "mag2 = data.$filt2\_vega\n";
	    print PRO "magerr1 = data.$filt1\_err\n";
	    print PRO "magerr2 = data.$filt2\_err\n";
	    print PRO "xpix = data.x\n";
	    print PRO "ypix = data.y\n";
	    print PRO "outfile=\'$outfile2\'\n";
	    print PRO "contcmd_err_grid,OUTFILE=outfile,MAG1=mag1,MAG2=mag2,MAGERR1=magerr1,MAGERR2=magerr2,BINSIZE=$gridsize,XMIN=$col1,YMIN=$mag1,XMAX=$col2,YMAX=$mag2,LEVELS=\[$clevels\],THRESHOLD=$threshold,XLABEL='$filt1-$filt2',YLABEL='$filt2',GRID=$gridbreak,XPIXELS=data.x,YPIXELS=data.y,PLOTTITLE='$targname'\n";
	}
    }
    close PRO;
    system "idl < $pro >> $logfile 2>&1\n";
    &logprint("idl < $pro >> $logfile 2>&1\n");
    
    unless (-e $outfile){
	die "NO COLOR MAGNITUDE DIAGRAM CREATED FOR $filt1 and $filt2 IN $gal\n";
    }
    &logprint("committing cmd files to s3...\n");
    my $dataproducts = $pipeline->selectBy($this_configuration, ['suffix','.ps']);
    $pipeline->s3commit($dataproducts);
    &logprint("files committed...\n");
    $cmdpar = $filt1 . "_" . $filt2 . "_cmd";
    $this_configuration->addParameter($cmdpar, $outfile);
}

$cmd_evt = acs2::jpipecore::Event->getEvent($thisjob,
          "new_cmds_done", 0);
$cmd_evt->setOption("target_id", $this_target->getId());
$pipeline->fire($cmd_evt);
&logprint("Fired new_cmds_done event ",$cmd_evt->getId(),"\n");



    close LOG;
exit 0;

 
    
    

