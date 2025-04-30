/*
Apr 8, 2025 16:46
calcualte CAR for each news publish date from datago
unable to run efficiently?

Apr 28, 2025 22:50
stkcd date specific calculation
slice stkcd-date to reduce memory usage

Apr 29, 2025 15:26
keep AR information
*/
clear all
set processor `c(processors_max)'
global root_dir "/Users/zch/projects/calculateCAR"
cd $root_dir
/* global data_dir "/Volumes/T7Shield/data/CSMAR股票交易数据" */
global data_dir "/Users/zch/projects/data/CSMAR股票交易数据"

**************************************************
**************************************************
**# Program
**************************************************
**************************************************
// 根据stkcd date_anno进行merge stata joinby？
capture program drop prepare_car_data
program define prepare_car_data
args infile outfile year_event pre_esitmation post_estimation post_event least_points
// 优化内存处理 只保留前后一年数据
use "data/stkcd_ret99-23.dta", clear
gen year = year(date)
keep if year>=`year_event'-1 & year<=`year_event'+1
drop year
save "data/temp.dta", replace

use "`infile'", clear
keep if year == `year_event'  // 会计年度 用于区分事件
duplicates drop _all, force
// 多对多使用joinby而不是merge 
/* joinby stkcd using "data/stkcd_ret99-23.dta" */
joinby stkcd using "data/temp.dta"
/* merge m:1 stkcd using "data/stkcd_ret99-23.dta", gen(_merge) */
/* keep if _merge==3 */
/* drop _merge */
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
// 去除事件5天内没有交易的股票
// trade war paper
// we also drop firms with any trading suspension during the five day event window centered on the event date to ensure that the event is fully incorporated into the stock price
drop if date_diff_trade_anno>5
keep stkcd date_trade date_diff_trade_anno
rename date_trade date_t0
label var date_t0 "Closest Trading Date to Announcement Date"
label var date_diff_trade_anno "Days between Trading Date and Announcement Date"
// merge individual return
/* joinby stkcd using "data/stkcd_ret99-23.dta" */
joinby stkcd using "data/temp.dta"
/* merge 1:m stkcd using "data/stkcd_ret99-23.dta", gen(_merge)
keep if _merge==3
drop _merge */
// merge with market return
merge m:1 date_trade using "data/market_return91-23.dta", gen(_merge)
keep if _merge==3
drop _merge
gen date_diff = date_trade - date_t0
keep if date_diff>=`pre_esitmation'&date_diff<=`post_event'
gen temp = 1 if date_diff<=`post_estimation'
bys stkcd date_t0: egen total_dates = sum(temp)
drop if total_dates<`least_points'
drop temp total_dates
save "`outfile'", replace
end


capture program drop market_model
program define market_model
args infile outfile pre_esitmation post_estimation post_event
use "`infile'", clear
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
save "`outfile'", replace
end

capture program drop calculateCAR
program define calculateCAR
args infile outfile max_window
use "`infile'", clear
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
// 保留ar信息
duplicates drop id, force
// idiosyncratic 暂时没用
drop alpha* beta* date_diff mkret* dretwd* ar_* R2* idiosyncratic* estimation_window
save "`outfile'", replace
end

********************************************************************************
********************************************************************************
**#  0. Set up
********************************************************************************
********************************************************************************
global out_dir "data/datago"
cap: mkdir $out_dir
global event_path "/Users/zch/projects/data/datago-media/news_basic_pubdate98-20"
global date_dir "data/datago_dates"
cap: mkdir $date_dir


// slice the data into sub-samples, each containing 1w observations
forvalues y=2000(1)2020{
    di "`y' Start `c(current_time)' `c(current_date)'"
qui{
    use "$event_path", clear
    sort year stkcd date_anno, stable
    keep if year==`y'
    gen id = _n
    sum id
    local n = r(max)
    local n1 = floor(`n'/10000)+1
    forvalues i=1(1)`n1'{
        preserve
        keep if id>=(`i'-1)*10000+1 & id<=`i'*10000
        save "$date_dir/`y'_`i'.dta", replace
        restore
    }
}
    di "`y', Finished, `n', `n1', `c(current_time)' `c(current_date)'"
}

**************************************************
**************************************************
**# 1. Datago Pub Date Estimation data
**************************************************
**************************************************
// stkcd year date_anno
// see /Users/zch/projects/media/code/prepare_datago_raw.do
global out_dir "data/datago"
cap: mkdir $out_dir
global event_path "/Users/zch/projects/data/datago-media/news_basic_pubdate98-20"

local pre_esitmation -365
local post_estimation -31
local post_event = 5
local max_window = 5
local min_trade_days = 100
forvalues year=2000(1)2020{
    di "------`year'------  Start `c(current_time)' `c(current_date)'"
qui{
    prepare_car_data "$event_path" "$out_dir/event`year'.dta" `year' `pre_esitmation' `post_estimation' `post_event' `min_trade_days'
    market_model "$out_dir/event`year'" "$out_dir/mkt_model`year'" `pre_esitmation' `post_estimation' `post_event'
    calculateCAR "$out_dir/mkt_model`year'" "$out_dir/CAR`year'" `max_window'
}
}
// combine data
use "$out_dir/CAR2000", clear
forvalues year = 2001(1)2020{
    append using "$out_dir/CAR`year'"
}
drop date_trade id
// accounting year
save "$out_dir/CAR2000-2020", replace



// 根据切片merge计算CAR 需要计算很久
/* local files: ls "data/datago_dates/2000*.dta"
di "`files'" */
TODO 计算CAR
local filelist : dir "data/datago_dates" files "2000*.dta"
foreach x in `filelist'{
    // pass
}


**************************************************
**************************************************
**# 2. Market Model
**************************************************
**************************************************
/* global out_dir "data/datago"
local pre_esitmation -365
local post_estimation -31
local post_event = 3
local max_window = 3
forvalues year = 2000(1)2020{
    di "------`year'------ `c(current_time)' `c(current_date)'"
qui{
    market_model "$out_dir/event`year'" "$out_dir/mkt_model`year'" `pre_esitmation' `post_estimation' `post_event'
    calculateCAR "$out_dir/mkt_model`year'" "$out_dir/CAR`year'" `max_window'
}
}

di "------2020------ `c(current_time)' `c(current_date)'" */


**************************************************
**************************************************
**# temp
**************************************************
**************************************************
local year_event = 2000
use "data/stkcd_ret99-23.dta", clear
gen year = year(date)
keep if year>=`year_event'-1 & year<=`year_event'+1
drop year
save "data/temp.dta", replace

use "$event_path" , clear
keep if year == `year_event'  // 会计年度 用于区分事件
duplicates drop _all, force
joinby stkcd using "data/temp.dta"
// 3GB
gen date_diff_trade_anno = date_trade - date_anno
keep if date_diff_trade_anno>=0
bys stkcd date_anno: egen min_date_diff_trade_anno = min(date_diff_trade_anno)
keep if date_diff_trade_anno==min_date_diff_trade_anno
duplicates drop _all, force
drop min_date_diff_trade_anno 
drop if date_diff_trade_anno>5
keep stkcd date_trade date_diff_trade_anno
rename date_trade date_t0
label var date_t0 "Closest Trading Date to Announcement Date"
label var date_diff_trade_anno "Days between Trading Date and Announcement Date"
// merge individual return
/* joinby stkcd using "data/stkcd_ret99-23.dta" */
joinby stkcd using "data/temp.dta"
/* merge 1:m stkcd using "data/stkcd_ret99-23.dta", gen(_merge)
keep if _merge==3
drop _merge */
// merge with market return
merge m:1 date_trade using "data/market_return91-23.dta", gen(_merge)
keep if _merge==3
drop _merge
gen date_diff = date_trade - date_t0
local pre_esitmation -365
local post_estimation -31
local post_event = 3
local least_points = 100
keep if date_diff>=`pre_esitmation'&date_diff<=`post_event'
gen temp = 1 if date_diff<=`post_estimation'
bys stkcd date_t0: egen total_dates = sum(temp)
drop if total_dates<`least_points'
drop temp total_dates
save "$out_dir/event2000.dta", replace



// market model
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
local pre_esitmation -365
local post_estimation -31
gen estimation_window = 1 if date_diff>=`pre_esitmation'&date_diff<=`post_estimation'
cap: drop d
cap: drop new
local n = r(max)
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



// announcement date
// follow Liu Shu Wei JFE
/* global out_dir "data/datago"
cap: mkdir $out_dir

local infile "stkcd_anno_date.dta"
use "data/`infile'", clear

prepare_car_data `infile' "$out_dir/event2010.dta" 2010 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2011.dta" 2011 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2012.dta" 2012 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2013.dta" 2013 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2014.dta" 2014 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2015.dta" 2015 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2016.dta" 2016 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2017.dta" 2017 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2018.dta" 2018 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2019.dta" 2019 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2020.dta" 2020 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2021.dta" 2021 -365 -31 10 100
prepare_car_data `infile' "$out_dir/event2022.dta" 2022 -365 -31 10 100 */