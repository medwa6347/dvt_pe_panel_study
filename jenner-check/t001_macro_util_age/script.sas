/*--------------------------------------------------------------------*\
| Bundle: exercise util_age (amdg/00_macros/util_age.sas) unmodified.    |
| The macro below is a byte-for-byte copy of the repo's util_age.sas;    |
| a small caller supplies mock DOB rows so the exact-age calculation     |
| runs in isolation. No repo logic is altered.                          |
\*--------------------------------------------------------------------*/

%macro util_age(varname=Age, From_Dt=, To_Dt=);
 /*-----------------------------------------------------------------*\
 | MACRO TO CALCULATE EXACT AGE IN YEARS                             |
 | Author: Felix Friedman 2013-08-19                                 |
 |                                                                   |
 | USE THIS MACRO WITHIN A DATA STEP WHICH CONTAINS DATE VARS TO     |
 | CALCULATE AGE FROM.  THIS IS AN EXAMPLE OF USING THIS MACRO:      |
 |    %age(varname=Age, From_Dt=DOB, To_Dt=today());                 |
 \*-----------------------------------------------------------------*/
   length &varname 3;
   &varname = int((
                   intck('month', &From_Dt, &To_Dt)-(day(&To_Dt)<min(day(&From_Dt),
                   day(intnx('month',&To_Dt, 1)-1)))
                  )/12);
%mend;

/* Mock member DOB / index rows to exercise the exact-age math, including
   a birthday-not-yet-reached case and a leap-day birth. */
data members;
   infile datalines dsd;
   input member_id :$4. DOB :yymmdd10. Index_Dt :yymmdd10.;
   format DOB Index_Dt yymmdd10.;
datalines;
M001,1950-06-15,2018-07-31
M002,1980-08-01,2018-07-31
M003,1980-07-31,2018-07-31
M004,2000-02-29,2018-02-28
M005,1972-12-31,2018-01-01
;
run;

data members_aged;
   set members;
   %util_age(varname=Age, From_Dt=DOB, To_Dt=Index_Dt);
run;

proc print data=members_aged noobs;
   var member_id DOB Index_Dt Age;
   title "util_age: exact age in years at index date";
run;
