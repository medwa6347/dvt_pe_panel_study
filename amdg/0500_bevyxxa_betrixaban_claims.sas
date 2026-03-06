 /*----------------------------------------------------------------*\
 | STANDALONE ADHOC BEVYXXA_BETRIXABAN ANALYSIS FOR BAYER - CLAIMS	|
 |  HTTP://DMO.OPTUM.COM/PRODUCTS/NHI.HTML													|
 | AUTHOR: MICHAEL EDWARDS 2018-08-03 AMDG                          |
 \*----------------------------------------------------------------*/													
/**/
                     
* COMMAND LINE;								
/*
cd /hpsaslca/mwe/bayer/bevyxxa_betrixaban/amdg
sas_tws 0500_bevyxxa_betrixaban_claims.sas -autoexec /hpsaslca/mwe/bayer/bevyxxa_betrixaban/amdg/00_common/00_common.sas &                                                       
*/      

%macro data_claims;
	
%local vz; %let vz = 1;

*COMMON - REDUNDANT, FOR EXECUTION ON SAS EG;
%include "/hpsaslca/mwe/bayer/bevyxxa_betrixaban/amdg/00_common/00_common.sas";

*NHI MX, RX CLAIM FIELDS;
%include "&om_code./0501_claims_flds.sas";

/*---------------------------------------------------------------------*/
/*---> DEFINE NHI CONNECTION <-----------------------------------------*/
/**/
%local nhi_sbox nhi_view nhi_specs u mcr mcr_cohort com com_cohort;
%let NHI_Specs = user="&un_unix." password="&pw_unix." server="NHIProd";
%let nhi_sbox = NHIPDHMMSandbox;
%let nhi_view = STATEVIEW;
libname _sbox_ teradata &NHI_Specs schema="&nhi_sbox";
*DELETE ANY LEFTOVER NHI SANDBOX DATA;
proc datasets nolist library=_sbox_; delete t&vz.:; quit;  
proc datasets nolist library=_sbox_; delete td&vz.:; quit; 

/*-----------------------------------------------------------------*/
/*---> BEVYXXA_BETRIXABAN MX CLAIMS <------------------------------*/
/**/

*CLEAR OUT NHI SPOOL SPACE;
%util_nhi_clear_spool;

%put NOTE: Pulling Market Claims...;

%let mx_tbl =  facility_claim;
%include "&om_code./0501_claims_flds.sas";

%let mx_claims_whr = 
         where (ndc.brand_name like ('%BEVYXXA%') or ndc.generic_name like ('%BEVYXXA%') or ndc.brand_name like ('%BETRIXABAN%') or ndc.generic_name like ('%BETRIXABAN%') or ndc.code in ('69853020101')) 
         	 and mx.service_from_date between '2017-01-01' and '2018-07-31'; 

%let mx_claims = &mx_claims_flds. &mx_claims_from. &mx_claims_whr.;

* CREATE TEMPORARY MARKET MEMBERS TABLE IN NHI SANDBOX; 
%put Facility Claims...;
%put ;
proc sql noerrorstop;
   *----------------------------------------------------------------*;
   *---> DEFINE CONNECTIONS TO NHI DATABASE;
   connect to teradata as nhi_sbox(&NHI_Specs schema="&nhi_sbox" mode=teradata);
   connect to teradata as nhi_view(&NHI_Specs schema="&nhi_view" mode=teradata);
   *----------------------------------------------------------------*;
   *---> EXTRACT MARKET MEMBERS;
   execute(
      create table &nhi_sbox..t&vz._fac_claims as (
         
				&mx_claims.				
				  	      		
			) with data
   ) by nhi_sbox;       
   disconnect from nhi_sbox;
   disconnect from nhi_view;
quit;

data inp.t&vz._fac_claims; set _sbox_.t&vz._fac_claims; run;
proc datasets nolist library=_sbox_; delete t&vz._fac_claims; quit;  

*CLEAR OUT NHI SPOOL SPACE;
%util_nhi_clear_spool;

%let mx_tbl =  physician_claim;
%include "&om_code./0501_claims_flds.sas";
%let mx_claims = &mx_claims_flds. &mx_claims_from. &mx_claims_whr.;

* CREATE TEMPORARY MARKET MEMBERS TABLE IN NHI SANDBOX; 
%put Physician claims...;
%put ;
proc sql noerrorstop;
   *----------------------------------------------------------------*;
   *---> DEFINE CONNECTIONS TO NHI DATABASE;
   connect to teradata as nhi_sbox(&NHI_Specs schema="&nhi_sbox" mode=teradata);
   connect to teradata as nhi_view(&NHI_Specs schema="&nhi_view" mode=teradata);
   *----------------------------------------------------------------*;
   *---> EXTRACT MARKET MEMBERS;
   execute(
      create table &nhi_sbox..t&vz._phy_claims  as (
				
				&mx_claims.
				  	      		
			) with data
   ) by nhi_sbox;       
   disconnect from nhi_sbox;
   disconnect from nhi_view;
quit; 

data inp.t&vz._phy_claims; set _sbox_.t&vz._phy_claims; run;
proc datasets nolist library=_sbox_; delete t&vz._phy_claims; quit;  

data inp.final_bnb_mx_claims;
	length &mx_var_length.; 
	set inp.t&vz._fac_claims 
			inp.t&vz._phy_claims;  
run;
proc sort data=inp.final_bnb_mx_claims; by individual_id service_from_date claim_id; run;	

/*-----------------------------------------------------------------*/
/*---> SOLARIS RX CLAIMS <-----------------------------------------*/
/**/

	%let rx_com_claims = 
         select distinct
				   	rx.nhi_individual_id														as individual_id    					
				  , rx.nhi_claim_nbr 			  												as claim_nbr                    
				  , rx.fill_date                             				as fill_date                      
				  , ndc.code                              					as ndc                          
				  , rx.count_days_supply                     				as days_supply    
				  , rx.quantity_drug_units													as quantity_drug_units                
  				, pj.specialty_category_code 											as rx_prov_sp_code
				  , rx.amt_copay+rx.amt_deductible           				as copay_rx                     
				  , rx.amt_paid                              				as tot_allowed_rx               
				  , rx.specialty_phmcy                     					as specialty_ind           
				  , case when 
				  		rx.mail_order_ind in ('Y') 
				  			then 'M' 
				  		when rx.retail_phmcy	in ('Y') 
				  			then 'R' 
				  			else 'O' end 																as rx_location
				  , ndc.generic_ind																	as generic_ind
				  , case when 
				  		ndc.generic_ind = 1 
				  			then 'G' 
				  			else 'B'	end																as generic_desc
				  , ndc.ahfs_therapeutic_class_desc									as ahfs_class_desc
				  , ndc.brand_name																	as brand_name
				  , ndc.generic_name																as generic_name
           from &nhi_view..pharmacy_claim 									rx 
				 inner join member_coverage_month										mbr
    			 on rx.nhi_member_system_id=mbr.nhi_member_system_id
				 inner join 																				ndc 																									
				 	on rx.ndc_key = ndc.ndc_key
 		 		 left outer join provider 														pj 
					on rx.prescribing_provider_key = pj.provider_key
    		 where (ndc.brand_name like ('%BEVYXXA%') or ndc.generic_name like ('%BEVYXXA%') or ndc.brand_name like ('%BETRIXABAN%') or ndc.generic_name like ('%BETRIXABAN%') or ndc.code in ('69853020101'))
    		 	 and rx.fill_date between '2017-01-01' and '2018-07-31'; 

%let rx_mcr_claims = 
       	 select distinct 
				   	rx.nhi_individual_id														as individual_id    					
				  , rx.nhi_claim_nbr 			  												as claim_nbr                    
				  , rx.fill_date                             				as fill_date                      
				  , ndc.code                              					as ndc                          
				  , rx.count_days_supply                     				as days_supply                    
				  , rx.quantity_drug_units													as quantity_drug_units                
  				, pj.specialty_category_code 											as rx_prov_sp_code
				  , rx.amt_copay+rx.amt_deductible           				as copay_rx                     
				  , rx.amt_paid                              				as tot_allowed_rx               
				  , rx.specialty_phmcy                     					as specialty_ind           
				  , case when 
				  		rx.mail_order_ind in ('Y') 
				  			then 'M' 
				  		when rx.retail_phmcy	in ('Y') 
				  			then 'R' 
				  			else 'O' end 																as rx_location
				  , ndc.generic_ind																	as generic_ind
				  , case when 
				  		ndc.generic_ind = 1 
				  			then 'G' 
				  			else 'B'	end																as generic_desc
				  , ndc.ahfs_therapeutic_class_desc									as ahfs_class_desc
				  , ndc.brand_name																	as brand_name
				  , ndc.generic_name																as generic_name				  
         from &nhi_view..pharmacy_claim_partd 							rx 
				 inner join member_coverage_month_partd							mbr
    			 on rx.nhi_member_system_id=mbr.nhi_member_system_id
				 inner join 																				ndc 																									
				 	on rx.ndc_key = ndc.ndc_key
 		 		 left outer join provider 													pj 
					on rx.prescribing_provider_key = pj.provider_key
    		 where (ndc.brand_name like ('%BEVYXXA%') or ndc.brand_name like ('%BETRIXABAN%') or ndc.generic_name like ('%BEVYXXA%') or ndc.generic_name like ('%BETRIXABAN%') or ndc.code in ('69853020101'))
    		   and rx.fill_date between '2016-01-01' and '2018-07-31'; 

* EXTRACT RX CLAIM DATA; 		
%put Extracting Commercial rx claim data...;
proc sql noerrorstop;
   *----------------------------------------------------------------*;
   *---> DEFINE CONNECTIONS TO NHI DATABASE;
   connect to teradata as nhi_sbox(&NHI_Specs schema="&nhi_sbox" mode=teradata);
   connect to teradata as nhi_view(&NHI_Specs schema="&nhi_view" mode=teradata);
   *----------------------------------------------------------------*;
   *---> CREATE RX CLAIM FILE;
   execute(
      create table &nhi_sbox..t&vz._in_rx_q_com as (
				
				&rx_com_claims
				
			) with data
   ) by nhi_sbox;  
   disconnect from nhi_sbox;
   disconnect from nhi_view;
quit;

data inp.t&vz._in_rx_q_com; length &rx_var_length.; set _sbox_.t&vz._in_rx_q_com;       

* EXTRACT RX CLAIM DATA; 		
proc sql noerrorstop;
   *----------------------------------------------------------------*;
   *---> DEFINE CONNECTIONS TO NHI DATABASE;
   connect to teradata as nhi_sbox(&NHI_Specs schema="&nhi_sbox" mode=teradata);
   connect to teradata as nhi_view(&NHI_Specs schema="&nhi_view" mode=teradata);
   *----------------------------------------------------------------*;
   *---> CREATE RX CLAIM FILE;
   execute(
      create table &nhi_sbox..t&vz._in_rx_q_mcr as (

				&rx_mcr_claims

			) with data
   ) by nhi_sbox;  
   disconnect from nhi_sbox;
   disconnect from nhi_view;
quit;  

data inp.t&vz._in_rx_q_mcr; length &rx_var_length.; set _sbox_.t&vz._in_rx_q_mcr;               

data inp.final_bnb_rx_claims;
	length &rx_var_length.; 
	set inp.t&vz._in_rx_q_com 
			inp.t&vz._in_rx_q_mcr;  
run;
proc sort data=inp.final_bnb_rx_claims; by individual_id fill_date ndc; run;
	
/*------> DELETE ANY LEFTOVER DATA <-------------------------------*/
/**/
*DELETE ANY LEFTOVER DATA;
proc datasets nolist; delete t&vz.:; quit;
proc datasets nolist library=inp; delete t&vz.:; quit;  
proc datasets nolist library=inp; delete td&vz.:; quit; 
 
%mend;

/*-----------------------------------------------------------------*/
/*---> EXECUTE <---------------------------------------------------*/
/**/ 

%data_claims;
