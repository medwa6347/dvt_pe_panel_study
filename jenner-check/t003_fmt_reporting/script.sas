/*--------------------------------------------------------------------*\
| Bundle: exercise the reporting formats from                           |
| amdg/00_formats/fmt_reporting.sas. The PROC FORMAT value statements    |
| below are copied verbatim from the repo's fmt_reporting.sas (the       |
| study's dose / continuous-therapy / gaps / weight / dx / age / race    |
| groupers). A small DATA step applies them via PUT() so the ranges,     |
| overlaps and $-formats resolve exactly as authored. No logic altered.  |
\*--------------------------------------------------------------------*/

proc format;
	 value dose_fmt					29-31						= '01_:300mg'
 				 									59-61   				= '02_:600mg'
 				 									89-91   				= '03_:900mg'
 				 									119-121 				= '04_:1200mg'
 				 									179-181 				= '05_:1800mg'
 				 									239-241 				= '06_:2400mg'
 				 									269-271 				= '07_:2700mg'
 				 									359-361 				= '08_:3600mg'
 				 									479-481 				= '09_:4800mg'
 				 									other 					= '10_:Other Dose';
	value  cont_pct_fmt			0								= '11_:No Continuous Therapy'
				 									0.0001-0.19999	= '10_:10% Continuous Therapy'
				 									0.2-0.29999			= '09_:20% Continuous Therapy'
				 									0.3-0.39999			= '08_:30% Continuous Therapy'
				 									0.4-0.49999			= '07_:40% Continuous Therapy'
				 									0.5-0.59999			= '06_:50% Continuous Therapy'
				 									0.6-0.69999			= '05_:60% Continuous Therapy'
				 									0.7-0.79999			= '04_:70% Continuous Therapy'
				 									0.8-0.89999			= '03_:80% Continuous Therapy'
				 									0.9-0.99999			= '02_:90% Continuous Therapy'
				 									1								= '01_:100% Continuous Therapy';
	value  total_gaps_fmt		0								= '01_:No Gaps in Therapy'
				 									1-2							= '02_:1-2 Gaps in Therapy'
				 									3-6							= '03_:3-6 Gaps in Therapy'
				 									7-12						= '04_:7-12 Gaps in Therapy'
				 									13-high					= '05_:13 or More Gaps in Therapy';
	value	 pt_wt_fmt				low-<40					=	'01_: <40kg'
													40-60						=	'02_: 40-60kg'
													60<-100					=	'03_: 61-100kg'
													100<-high				=	'04_: >100kg';
	value	 $dx_fmt					D595						=	'PNH'
													D593						=	'aHUS'
													G7000,G7001			=	'gMG'
													other						=	'Other';
  value  age_fmt					low-<18					= 'Less than 18'
 													18-34   				= '18 to 34'
 													35-49   				= '35 to 49'
 													50-64   				= '50 to 64'
 													65-79   				= '65 to 79'
 													80-high 				=	'80+';
  value  age_test_fmt			low-<49					= 'Less than 50'
 													50-high 				=	'Over 50';
 	value	 $race_fmt				'AFRICAN AMERICAN'
 												 ,'ASIAN'
 												 ,'OTHER/UNKNOWN' = 'NOT CAUCASIAN'
 													'CAUCASIAN'			=	"CAUCASIAN";
run;

/* Apply the study's groupers to a handful of representative rows,
   including range-boundary values (60, 40, dose=90) and an unmapped dx. */
data reporting_demo;
   infile datalines dsd;
   input dose cont_pct total_gaps wt age dx :$6. race :$16.;
   dose_grp    = put(dose,       dose_fmt.);
   cont_grp    = put(cont_pct,   cont_pct_fmt.);
   gaps_grp    = put(total_gaps, total_gaps_fmt.);
   wt_grp      = put(wt,         pt_wt_fmt.);
   age_grp     = put(age,        age_fmt.);
   age_test    = put(age,        age_test_fmt.);
   dx_grp      = put(dx,         $dx_fmt.);
   race_grp    = put(race,       $race_fmt.);
datalines;
90,0.85,5,60,72,D595,CAUCASIAN
600,0,0,39,17,G7001,ASIAN
1200,1,15,101,80,D593,AFRICAN AMERICAN
77,0.35,2,40,49,X999,OTHER/UNKNOWN
;
run;

proc print data=reporting_demo noobs;
   var dose_grp cont_grp gaps_grp wt_grp age_grp age_test dx_grp race_grp;
   title "fmt_reporting: study reporting groupers applied";
run;
