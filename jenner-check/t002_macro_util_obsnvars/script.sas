/*--------------------------------------------------------------------*\
| Bundle: exercise util_obsnvars (amdg/00_macros/util_obsnvars.sas)      |
| unmodified. The macro below is a byte-for-byte copy of the repo's      |
| util_obsnvars.sas; two callers exercise its OPEN/ATTRN/CLOSE path      |
| against a populated dataset and an empty one, then surface the         |
| global macro variables it sets. No repo logic is altered.             |
\*--------------------------------------------------------------------*/

 /*----------------------------------------------------------------*\
 | MACRO TO RETURN OBS AND VARS FROM DS															|
 | AUTHOR: Felix Friedman 2013-02-05                          			|
 | MODIFIED: Michael Edwards 2017-05-03															|
  \*---------------------------------------------------------------*/
/**/

%macro util_obsnvars(ds,req=);
%* 2013-02-05 Felix: this is a modified version of example from SAS
   v9.2 help file in %sysfunc chapter.;
%global dset nvars nobs ds_empty ds_exist;
%local wordvars wordobs;
%let dset=&ds;
%let dsid = %sysfunc(open(&dset));
%if &dsid %then %do;
   %let ds_exist = 1;
   %let nobs =%sysfunc(attrn(&dsid,NOBS));
   %let nvars=%sysfunc(attrn(&dsid,NVARS));
   %let rc = %sysfunc(close(&dsid));
   %*put &dset has &nvars variable(s) and &nobs observation(s).;
   %if &nvars=1 %then %let wordvars=variable; %else %let wordvars=variables;
   %if &nobs=1 %then %let wordobs=observation; %else %let wordobs=observations;
   %put NOTE: &dset has &nvars &wordvars and &nobs &wordobs..;
   %if &nobs>&req %then %let ds_empty = 0; %else %let ds_empty = 1;
   %end;
%else %do; %let ds_exist = 0; %let ds_empty = 1; %put WARNING: Open for data set &dset failed.; %end;
%mend;

/* A small mock claims-style table (individual_id + a few dx columns),
   echoing the kind of member-level data this study introspects. */
data claims_mock;
   infile datalines dsd;
   input individual_id :8. dx1 :$7. dx2 :$7. pos_cd :$2.;
datalines;
1001,I2699,,21
1002,I8220,I2699,22
1003,I82409,,23
;
run;

/* empty analog, same columns */
data claims_empty;
   set claims_mock;
   stop;
run;

%util_obsnvars(claims_mock, req=0);
%put NOTE: [caller] claims_mock -> ds_exist=&ds_exist nobs=&nobs nvars=&nvars ds_empty=&ds_empty;

%util_obsnvars(claims_empty, req=0);
%put NOTE: [caller] claims_empty -> ds_exist=&ds_exist nobs=&nobs nvars=&nvars ds_empty=&ds_empty;
