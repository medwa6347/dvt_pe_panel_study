/*-----------------------------------------------------------------*/
/*---> EHR LENGTH STMTS <------------------------------------------*/
/**/
															
%let rx_var_length 				= 	                        panther_id			$11 					         
															drug_name           $25          
															ndc                 $11           
															admin_date          4             
															quantity_of_dose    $3           
															strength            $4           
															generic_desc        $10;   

/*-----------------------------------------------------------------*/
/*---> EHR SQL GENERATION <----------------------------------------*/
/**/

%macro ehr_flds_define;

*EHR RX ADMINS;
%global rx_flds rx_from;
%let rx_flds = select distinct 
														  rx.panther_id					
														, rx.drug_name           
														, rx.ndc                 
														, rx.rxdate as admin_date          
														, rx.quantity_of_dose    
														, rx.strength            
														, rx.generic_desc;			 
%let rx_from = from &nhi_view..clnpntr_prescriptions rx;

%mend;
%ehr_flds_define;