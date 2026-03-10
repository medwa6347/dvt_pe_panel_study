 /*----------------------------------------------------------------*\
 | STANDALONE ADHOC BEVYXXA BETRIXABAN ANALYSIS FOR BAYER - EHR	  |
 |  HTTP://DMO.OPTUM.COM/PRODUCTS/NHI.HTML								  |
 | AUTHOR: MICHAEL EDWARDS 2018-08-03 AMDG                          |
 \*----------------------------------------------------------------*/													
/**/
                     
* COMMAND LINE;								
/*
cd /hpsaslca/mwe/bayer/bevyxxa_betrixaban/amdg
sas_tws 0600_bevyxxa_betrixaban_ehr.sas -autoexec /hpsaslca/mwe/bayer/bevyxxa_betrixaban/amdg/00_common/00_common.sas &                                                       
*/      

%macro data_ehr;
	
%local vz; %let vz = 2;

*COMMON - REDUNDANT, FOR EXECUTION ON SAS EG;
%include "/hpsaslca/mwe/bayer/bevyxxa_betrixaban/amdg/00_common/00_common.sas";

/*---------------------------------------------------------------------*/
/*---> DEFINE NHI CONNECTION <-----------------------------------------*/
/**/
%local nhi_sbox nhi_view nhi_specs u mcr mcr_cohort com com_cohort;
%let nhi_view = CCISTATEVIEW;
%let NHI_Specs = user="&un_unix." password="&pw_unix." server="NHIProd";
%let nhi_sbox = NHIPDHMMSandbox;
libname _sbox_ teradata &NHI_Specs schema="&nhi_sbox";
*DELETE ANY LEFTOVER NHI SANDBOX DATA;
proc datasets nolist library=_sbox_; delete t&vz.:; quit;  
proc datasets nolist library=_sbox_; delete td&vz.:; quit; 

*NHI FIELDS;
%include "&om_code./0601_ehr_flds.sas";

/*----------------------------------------------------------------*/
/*---> BNB EHR RX <-----------------------------------------------*/
/**/

*CLEAR OUT NHI SPOOL SPACE;
%util_nhi_clear_spool;

%put NOTE: Pulling Market EHR-Rx Data...;

%let rx_whr = where (drug_name like ('%BEVYXXA%') or generic_desc like ('%BEVYXXA%') or drug_name like ('%BETRIXABAN%') or generic_desc like ('%BETRIXABAN%') or ndc in ('69853020101')) and (admin_date between '2017-01-01' and '2018-07-31'); 

%let rx = &rx_flds. &rx_from. &rx_whr.;

* NHI DATA PULL; 
%put EHR-Rx Data...;
%put ;
proc sql noerrorstop;
   *----------------------------------------------------------------*;
   *---> DEFINE CONNECTIONS TO NHI DATABASE;
   connect to teradata as nhi_sbox(&NHI_Specs schema="&nhi_sbox" mode=teradata);
   connect to teradata as nhi_view(&NHI_Specs schema="&nhi_view" mode=teradata);
   *----------------------------------------------------------------*;
   *---> EXTRACT MARKET MEMBERS;
   execute(
      create table &nhi_sbox..t&vz._rx as (
         
				&rx.				
				  	      		
			) with data
   ) by nhi_sbox;       
   disconnect from nhi_sbox;
   disconnect from nhi_view;
quit;

data inp.final_ehr_rx; length &rx_var_length.; set _sbox_.t&vz._rx; run;
proc datasets nolist library=_sbox_; delete t&vz._rx; quit; 

proc sort data=inp.final_ehr_rx; by panther_id admin_date; run;

*DELETE ANY LEFTOVER DATA;
proc datasets nolist; delete t&vz.:; quit;
proc datasets nolist library=inp; delete t&vz.:; quit;  
proc datasets nolist library=inp; delete td&vz.:; quit; 
x rm -rf "&om_data./05_out_rep/*.png";
 
%mend;

/*-----------------------------------------------------------------*/
/*---> EXECUTE <---------------------------------------------------*/
/**/ 

%data_ehr;
