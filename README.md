## Goal 

Can big data tools and High Performance Computing (HPC) resources benefit data- 
and compute-intensive statistical analyses in high energy physics (HEP) to improve time-to-physics? 

## Synopsis

We use Spark to implement an active Large Hadron Collider (LHC) analysis, 
searching for Dark Matter with the Compact Muon Solenoid (CMS) detector as 
our use case. Our input data is in HDF5 format, we provide a custom HDF5 to 
Spark DataFrame reader. We also provide several examples to manipulate 
multiple DataFrames using UDFs, aggregations, etc. We provide functions 
specific to the CMS data, and evaluate the performance on the supercomputing 
resources provided by National Energy Research Scientific Computing Center (NERSC). 


