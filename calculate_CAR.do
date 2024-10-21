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
global outdir "$root_dir/data/CAR_output"
**************************************************
**************************************************
**# program
**************************************************
**************************************************

**************************************************
**************************************************
**# calculate CAR
**************************************************
**************************************************
/* label var markettype "5=综合A股,10=综合B股,15=综合AB股,21=综合A股和创业板,31=综合AB股和创业板"
Markettype [市场类型] - 1=上证A股市场 (不包含科创板），2=上证B股市场，4=深证A股市场（不包含创业板），8=深证B股市场，16=创业板， 32=科创板，64=北证A股市场。 */
use "$data_dir/event2010.dta", clear 
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
// TODO here


