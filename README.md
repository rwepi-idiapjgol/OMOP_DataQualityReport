# OMOP DATA QUALITY REPORT

1. Open the Rproject and install libraries using renv::restore().

2. Run RunCode.R in your both CDM instances. You will need to execute it twice, changing the connection settings and "database" name to create a new output folder for each version of the cdm.
   
3. Run Rmarkdown file "Report_comparing.Rmd" to create the report.
