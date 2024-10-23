/*
Oct 21, 2024 20:36
Calculate CAR
data
*/
clear all
set processor 8
global root_dir "/Users/zch/projects/calculateCAR"
cd $root_dir
global data_dir "$root_dir/data/ready_for_calculation"
global out_dir "$root_dir/data/CAR_output"
cap: mkdir $out_dir
**************************************************
**************************************************
**# program
**************************************************
**************************************************
capture program drop market_model
program define market_model
args infile outfile pre_esitmation post_estimation post_event
use "$data_dir/`infile'", clear
/* label var markettype "5=综合A股,10=综合B股,15=综合AB股,21=综合A股和创业板,31=综合AB股和创业板"
Markettype [市场类型] - 1=上证A股市场 (不包含科创板），2=上证B股市场，4=深证A股市场（不包含创业板），8=深证B股市场，16=创业板， 32=科创板，64=北证A股市场。 */
// 根据markettype 区分
gen mkretew = mretew_SH if markettype==1
replace mkretew = mretew_SZ if markettype==4
replace mkretew = mretew_GEM if markettype==16
replace mkretew = mretew_STAR if markettype==32
replace mkretew = mretew_BJ if markettype==64
gen mkretvw = mretvw_SH if markettype==1  
replace mkretvw = mretvw_SZ if markettype==4
replace mkretvw = mretvw_GEM if markettype==16
replace mkretvw = mretvw_STAR if markettype==32
replace mkretvw = mretvw_BJ if markettype==64
// total market value instead of just outstanding shares
/* gen mkrettvw = mrettvw_SH if markettype==1
replace mkrettvw = mrettvw_SZ if markettype==4
replace mkrettvw = mrettvw_GEM if markettype==16
replace mkrettvw = mrettvw_STAR if markettype==32
replace mkrettvw = mrettvw_BJ if markettype==64 */
drop mretvw_* mretew_* mrettvw_* markettype
drop date_diff_trade_anno
egen id = group(stkcd date_t0)
sum id
local n = r(max)
gen R2_ew = .
gen R2_vw = .
gen beta_ew = .
gen beta_vw = .
gen alpha_ew = .
gen alpha_vw = .
gen idiosyncratic_ew = .
gen idiosyncratic_vw = .
gen estimation_window = 1 if date_diff>=`pre_esitmation'&date_diff<=`post_estimation'
cap: drop d
cap: drop new
forvalues i=1(1)`n'{
    display "-------`i'/`n'------"
qui{
    // ew
    reg dretwd mkretew if id==`i'&estimation_window==1
    predict new, resid
    egen d = sd(new) if id==`i'
    replace idiosyncratic_ew = d if id==`i'
    replace R2_ew = e(r2) if id==`i'
    matrix c = e(b)'
    /* svmat double c, name(bvector) */
    replace beta_ew = c[1,1] if id==`i'
    replace alpha_ew = c[2,1] if id==`i'
    drop d new
    // vw
    reg dretwd mkretvw if id==`i'&estimation_window==1
    predict new, resid
    egen d = sd(new) if id==`i'
    replace idiosyncratic_vw = d if id==`i'
    replace R2_vw = e(r2) if id==`i'
    matrix c = e(b)'
    /* svmat double c, name(bvector) */
    replace beta_vw = c[1,1] if id==`i'
    replace alpha_vw = c[2,1] if id==`i'
    drop d new
}
}
keep if date_diff>=-`post_event'&date_diff<=`post_event'
save "$out_dir/`outfile'", replace
end

capture program drop calculateCAR
program define calculateCAR
args infile outfile max_window
use "$out_dir/`infile'", clear
keep if date_diff>=-`max_window'&date_diff<=`max_window'
gen ar_ew = dretwd - (alpha_ew + mkretew * beta_ew)
gen ar_vw = dretwd - (alpha_vw + mkretvw * beta_vw)
gen ar_ewadj = dretwd - mkretew
gen ar_vwadj = dretwd - mkretvw
cap:drop id
egen id = group(stkcd date_t0)
// 对称的window should assert pre_event == -post_event
forvalues window=`max_window'(-1)1{
    keep if date_diff>=-`window'&date_diff<=`window'
    bys id: egen car`window'`window'_ew = total(ar_ew)
    bys id: egen car`window'`window'_vw = total(ar_vw)
    bys id: egen car`window'`window'_ewadj = total(ar_ewadj)
    bys id: egen car`window'`window'_vwadj = total(ar_vwadj)
}
keep if date_diff>=0&date_diff<=1
bys id: egen car01_ew = total(ar_ew)
bys id: egen car01_vw = total(ar_vw)
bys id: egen car01_ewadj = total(ar_ewadj)
bys id: egen car01_vwadj = total(ar_vwadj)
sort id date_diff
keep if date_diff==0
bys id: egen car00_ew = total(ar_ew)
bys id: egen car00_vw = total(ar_vw)
bys id: egen car00_ewadj = total(ar_ewadj)
bys id: egen car00_vwadj = total(ar_vwadj)
duplicates drop id, force
// idiosyncratic 暂时没用
drop alpha* beta* date_diff mkret* dretwd* ar_* R2* idiosyncratic* estimation_window
save "$out_dir/`outfile'", replace
end

**************************************************
**************************************************
**# Market Model Estimation
**************************************************
**************************************************
// follow Garfinkel and Sobokin 2006
// liu shu wei JFE
local pre_esitmation -365
local post_estimation -31
local post_event = 6
forvalues year = 2010(1)2020{
    market_model "event`year'" "mkt_model`year'" `pre_esitmation' `post_estimation' `post_event'
}

**************************************************
**************************************************
**# Calculate CAR
**************************************************
**************************************************
local max_window = 6
forvalues year = 2010(1)2020{
    di "------`year'------"
qui{
    calculateCAR "mkt_model`year'" "CAR`year'" `max_window'
}
}

/* calculateCAR "mkt_model2021" "CAR2021" 6
calculateCAR "mkt_model2022" "CAR2022" 6 */

**************************************************
**************************************************
**# Combine Data
**************************************************
**************************************************
use "$out_dir/CAR2010", clear
forvalues year = 2011(1)2020{
    append using "$out_dir/CAR`year'"
}
drop date_trade id
// accounting year
save "$out_dir/CAR2010-2020", replace