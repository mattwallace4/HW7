/*
Author: Matt Wallace
Course: ST555 (001)
Date Created: 4/2/2020
Purpose: To create and validate datasets from the EPA datasets.
Written in SAS code.
*/

/* setting up librefs and filerefs */
x 'cd L:\st555';
libname Results 'Results';
libname InputDS 'Data';
filename RawData 'Data';

x 'cd S:\Documents\My SAS Files\9.4\HW7';
libname HW7 '.';
filename HW7 '.';

ods _all_ close;
options fmtsearch = (InputDS);

/* Reading in the raw data files */
data hw7.EPADataSO2(drop=_:);
  infile RawData('EPA Data.csv') dlm=',' firstobs=7 truncover;
  input    siteid aqscode poc
        #2 _date $
		#3 _aqs $
		#4 _aqi $
		#5 _count $
		
  ;
  _date1=input(compress(_date,,'lu'),12.);
  date=_date1+21549; *adjusting the dates to be ready for conversion to sas dates. I properly format them when joining in a later step;
  aqs=input(compress(_aqs,,'lu'),12.);
  aqi=input(compress(_aqi,,'lu'),12.);
  count=input(compress(_count,,'lu'),12.);
run;

data hw7.EPADataO3(drop=_:);
  infile RawData('EPA Data (1).csv') dlm=',' firstobs=2;
  input _date $ siteid poc aqs aqi count aqscode;
  _date1=compress(_date,,'p');
  date=input(_date1,mmddyy6.);
run;

data hw7.EPADataCO(drop=i _date); 
  infile RawData('EPA Data (2).csv') dsd firstobs=6 truncover;
  input siteid aqscode poc @;
  do i = 1 to 244;
	input aqs aqi count @;
    _date=i;
	date=_date+21549;
	if not missing(aqs) then
	output;
  end;
run;

proc transpose data=InputDS.pm10 out=hw7.PM10; 
  by siteid aqscode poc;
  id Metric;
run;

/* Joining the concentration datasets */
data hw7.joinedconcentration(drop=_:);
  set hw7.EPADataCO(in=inCO)
      hw7.EPADataSO2(in=inSO2)
 	  hw7.EPADataO3(in=inO3)
	  hw7.PM10(in=inPM10 rename=(mean=aqs));
  attrib date label='Observation Date' format=yymmdd10.
         siteid label='Site ID'
		 poc label='Parameter Occurance Code (Instrument Number within Site and Parameter)'
		 aqscode label='AQS Parameter Code'
		 parameter label='AQS Parameter Name' length=$50.
		 aqsabb label='AQS Parameter Abbreviation' length=$4.
		 aqsdesc label='AQS Measurement Description' length=$40.
		 aqs label='AQS Observed Value'
		 aqi label='Daily Air Quality Index Value'
		 aqidesc label='Daily AQI Category'
		 count label='Daily AQS Observations'
		 percent label='Percent of AQS Observations (100*Observed/24)'
  ;
  if inO3 then do;
                parameter='Ozone';
                aqsabb='O3';
			    aqsdesc='Daily Max 8-hour Ozone Concentration';
			   end;
  else if inPM10 then do;
                   parameter='PM10 Total 0-10um STP';
                   aqsabb='PM10';
				   aqsdesc='Daily Mean PM10 Concentration';
				   if missing(aqs) then delete;
 				   _date=input(compress(_name_,,'lu'),12.);
                   date=_date+21549;
			     end;
   else if inCO then do;
                 parameter='Carbon monoxide'; 
                 aqsabb='CO';
				 aqsdesc='Daily Max 8-hour CO Concentration';
			   end;
    else if inSO2 then do;
                        parameter='Sulfur dioxide'; 
                        aqsabb='SO2';
						aqsdesc='Daily Max 1-hour SO2 Concentration';
					  end;
     
  percent=round(100*count/24,1);
  aqidesc=put(aqi,AQICAT.);
run;

/* setting up the sites and concentration files for merging  */
data hw7.sites(drop=_: stcode countycode sitenum cbsaname);
  set inputds.aqssites(rename=(localname=localName));
  _siteid=cat(stcode,put(countycode,z3.),put(sitenum,z4.));
  siteid=input(_siteid,12.);
  stabbrev=scan(cbsaname,2,',');
  cityname=scan(cbsaname,1,',');
run;

proc sort data=hw7.joinedconcentration out=hw7.sortedconcentration;
  by siteid;
run;

/* merging the sites and concentration data */
data hw7.sitesconc;
  merge hw7.sortedconcentration
        hw7.sites;
  by siteid;
  attrib localname label='Site Name'
		 lat label='Site Latitude'
		 long label='Site Longitude'
		 stabbrev label='State Abbreviation'
		 countyname label='County Name'
		 cityname label='City Name'
		 estabdate label='Site Established Date' format=yymmdd10.
		 closedate label='Site Closed Date' format=yymmdd10.
  ;
run;

/* sorting the data for the final merge */
proc sort data=inputds.methods out=hw7.sortedmethods;
  by aqscode;
run;

proc sort data=hw7.sitesconc out=hw7.sortedsitesconc;
  by aqscode;
run;

/* merging the methods with the previously merged file to create the final dataset */
data hw7.finalwallace hw7.finalwallace100;
  retain date siteid poc aqscode parameter aqsabb aqsdesc aqs aqi aqidesc count percent 
         mode collectdescr analysis mdl localName lat long stabbrev countyname cityname estabdate closedate;
  merge hw7.sortedsitesconc(in=inConc)
		hw7.sortedmethods(in=inMethods drop = parameter);
  by aqscode;
  attrib mode label='Measurement Mode'
         collectdescr label='Description of Collection Process'
		 analysis label='Analysis Technique'
		 mdl label='Federal Method Detection Limit'
  ;
  collectdescr=propcase(collectdescr);
  analysis=propcase(analysis);
  if inConc eq 1 and inMethods eq 1 then output hw7.finalwallace;
  if percent eq 100 then output hw7.finalwallace100;
run;

/* generating descriptor portion */
ods output position=hw7.wallacedesc(drop=member);
proc contents data = hw7.finalwallace varnum;
run;

/* validation */
proc compare base = Results.hw7finalduggins compare = hw7.finalwallace
             out = hw7.diffsfinal
			 outbase outcompare outdiff outnoequal
			 method = absolute criterion = 1E-15
			 noprint;
run;

proc compare base = Results.hw7dugginsdesc compare = hw7.wallacedesc
             out = hw7.diffsdesc
			 outbase outcompare outdiff outnoequal
			 method = absolute criterion = 1E-15
			 noprint;
run;

quit;
