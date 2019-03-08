# WFIRST Pipeline Backend Development
Classes and methods defined in wpipe.py are utilized by scripts in the various 
test_dir for automated job execution. Uses HDF5 for backend data storage.

wpipe2.py: Processes in wpipe.py now converted to use postgresSQL backend via
SQLalchemy, otherwise a functional equivalemt of the previous code.
