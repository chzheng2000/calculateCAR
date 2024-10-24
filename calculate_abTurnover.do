/*
Oct 22, 2024 12:43
prepare data for Abnormal turnover
Abnormal turnover, The market-adjusted turnover in the three-day window around the firm's earnings announcement date t (t-1, t+1) minus that over the non-event window (t-41, t-5). Market-adjusted turnover is the difference between the average daily turnover and the average of the market over the given window (Garfinkel 2009).
*/
clear all
set processor 8
global root_dir "/Users/zch/projects/calculateCAR"
cd $root_dir
global data_dir "/Volumes/T7Shield/data/CSMAR股票交易数据"
global out_dir "data/data_for_abTurnover"
cap:mkdir $out_dir
**************************************************
**************************************************
**# 1. market volume data
**************************************************
**************************************************
/* Dnshrtrd [日个股交易股数] - 0=没有交易量
Dnvaltrd [日个股交易金额] - A股以人民币元计，上海B以美元计，深圳B以港币计，0=没有交易量
Dsmvosd [日个股流通市值] - 计算公式为：个股的流通股数与收盘价的乘积，A股以人民币元计，上海B股以美元计，深圳B股以港币计，注意单位是千。（注：ABH同股，只计算A股总市值）
Clsprc [日收盘价] - A股以人民币元计，上海B以美元计，深圳B以港币计 */
capture program drop get_turnover_data
program define get_turnover_data
args infile outfile
insheet using "$data_dir/`infile'", clear
keep stkcd dsmvosd dnshrtrd clsprc trddt markettype
gen date_trade = date(trddt, "YMD")
format date_trade %td
drop trddt
gen shares = dsmvosd/clsprc
drop dsmvosd clsprc
label var shares "shares outstanding in 1000"
rename dnshrtrd volume
keep if inlist(markettype, 1, 4, 16, 32, 64)
gen turnover = volume/(shares*1000)
// 计算市场平均
bys markettype date_trade: egen total_shares = sum(shares)
bys markettype date_trade: egen total_volume = sum(volume)
gen turnover_mkt = total_volume/(total_shares*1000)
save "$out_dir/`outfile'", replace
end

get_turnover_data "日个股回报率04-08/TRD_Dalyr.csv" "turnover04-08"
get_turnover_data "日个股回报率04-08/TRD_Dalyr1.csv" "turnover04-08_1"

get_turnover_data "日个股回报率09-13/TRD_Dalyr.csv" "turnover09-13"
get_turnover_data "日个股回报率09-13/TRD_Dalyr1.csv" "turnover09-13_1"
get_turnover_data "日个股回报率09-13/TRD_Dalyr2.csv" "turnover09-13_2"

get_turnover_data "日个股回报率14-18/TRD_Dalyr.csv" "turnover14-18"
get_turnover_data "日个股回报率14-18/TRD_Dalyr1.csv" "turnover14-18_1"
get_turnover_data "日个股回报率14-18/TRD_Dalyr2.csv" "turnover14-18_2"
get_turnover_data "日个股回报率14-18/TRD_Dalyr3.csv" "turnover14-18_3"


get_turnover_data "日个股回报率19-23/TRD_Dalyr.csv" "turnover19-23"
get_turnover_data "日个股回报率19-23/TRD_Dalyr1.csv" "turnover19-23_1"
get_turnover_data "日个股回报率19-23/TRD_Dalyr2.csv" "turnover19-23_2"
get_turnover_data "日个股回报率19-23/TRD_Dalyr3.csv" "turnover19-23_3"
get_turnover_data "日个股回报率19-23/TRD_Dalyr4.csv" "turnover19-23_4"
get_turnover_data "日个股回报率19-23/TRD_Dalyr5.csv" "turnover19-23_5"


use "$out_dir/turnover09-13.dta", clear
append using "$out_dir/turnover09-13_1.dta"
append using "$out_dir/turnover09-13_2.dta"
append using "$out_dir/turnover14-18.dta"
append using "$out_dir/turnover14-18_1.dta"
append using "$out_dir/turnover14-18_2.dta"
append using "$out_dir/turnover14-18_3.dta"
append using "$out_dir/turnover19-23.dta"
append using "$out_dir/turnover19-23_1.dta"
append using "$out_dir/turnover19-23_2.dta"
append using "$out_dir/turnover19-23_3.dta"
append using "$out_dir/turnover19-23_4.dta"
append using "$out_dir/turnover19-23_5.dta"
save "$out_dir/turnover09-23.dta", replace
**************************************************
**************************************************
**# 2. merge event date
**************************************************
**************************************************
capture program drop prepare_abturnover_data
program define prepare_abturnover_data
args infile outfile year_acc pre_esitmation post_event
use "data/`infile'", clear
keep if year_acc == `year_acc'  // 会计年度 用于区分事件
merge 1:m stkcd using "$out_dir/turnover09-23.dta", gen(_merge)
keep if _merge==3
drop _merge
/* // 缩小样本 如果内存不够的话
gen year_trade = year(date_trade)
keep if year_trade>=`year_acc'-2 & year_trade<=`year_acc'+1
drop year_trade */
gen date_diff_trade_anno = date_trade - date_anno
keep if date_diff_trade_anno>=0
bys stkcd date_anno: egen min_date_diff_trade_anno = min(date_diff_trade_anno)
keep if date_diff_trade_anno==min_date_diff_trade_anno
duplicates drop _all, force
drop min_date_diff_trade_anno 
// 去除事件10天内没有交易的股票
drop if date_diff_trade_anno>10
keep stkcd date_trade date_diff_trade_anno
rename date_trade date_t0
label var date_t0 "Closest Trading Date to Announcement Date"
label var date_diff_trade_anno "Days between Trading Date and Announcement Date"
// merge individual return
merge 1:m stkcd using "$out_dir/turnover09-23.dta", gen(_merge)
keep if _merge==3
drop _merge
gen date_diff = date_trade - date_t0
keep if date_diff>=`pre_esitmation'&date_diff<=`post_event'
save "$out_dir/`outfile'", replace
end

// follow Garfinkel&Sokobin 2006 JAR
local pre_esitmation -52
local post_estimation -5
local post_event 5
local infile "stkcd_anno_date.dta"
forvalues year =2010(1)2022{
	prepare_abturnover_data `infile' "event`year'.dta" `year' `pre_esitmation' `post_event'
}

**************************************************
**************************************************
**# Calculate abnormal turnover
**************************************************
**************************************************
capture program drop calculate_abTurnover
program define calculate_abTurnover
args infile outfile pre_esitmation post_estimation max_window
use "$out_dir/`infile'", clear
gen estimation_window = 1 if date_diff>=`pre_esitmation'&date_diff<=`post_estimation'
egen id = group(stkcd date_t0)
gen turnover_mktadj = turnover - turnover_mkt
// estimation
gen turnover_temp = .
replace turnover_temp = turnover_mktadj if estimation_window==1
bys id: egen turnover_mktadj_est = mean(turnover_temp)
forvalues i=1(1)`max_window'{
    gen turnover_temp`i' = .
    replace turnover_temp`i' = turnover_mktadj if date_diff>=-`i'&date_diff<=`i'
    bys id: egen turnover_mktadj`i'`i' = mean(turnover_temp`i')
    gen abTurnover`i'`i' = turnover_mktadj`i'`i' - turnover_mktadj_est
}
duplicates drop id, force
keep stkcd date_t0 abTurnover* turnover_mktadj*
save "$out_dir/`outfile'", replace
end

local pre_esitmation -52
local post_estimation -5
local max_window 5
forvalues year=2010(1)2022{
    calculate_abTurnover "event`year'.dta" "abTurnover`year'.dta" `pre_esitmation' `post_estimation' `max_window'
}


// merge data
use "$out_dir/abTurnover2010.dta", clear
forvalues year=2011(1)2022{
    append using "$out_dir/abTurnover`year'.dta"
}
duplicates drop stkcd date_t0, force
save "$out_dir/abTurnover2010-2022.dta", replace