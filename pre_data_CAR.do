/*
preprocess data for CAR calculation from CSMAR
*/
clear all
set processor 8
global root_dir "/Users/zch/projects/calculateCAR"
cd $root_dir
global data_dir "/Volumes/T7Shield/data/CSMAR股票交易数据"
**************************************************
**************************************************
**# 1. preprocess trading date data
**************************************************
**************************************************
insheet using "$data_dir/年中季报披露日期91-24/IAR_Rept.csv", clear
// 1=第一季度季报，2=中报，3=第三季度季报，4=年报
keep if reptyp==4
gen year_acc = substr(accper, 1, 4)
destring year_acc, replace
keep stkcd annodt year_acc
gen date_anno = date(annodt, "YMD")
format date_anno %td
label var date_anno "Annual Report Announcement Date"
drop annodt
gen year_anno = year(date_anno)
// duplication
drop if stkcd==600652&year_acc==1990&year_anno==1992
drop if date_anno==.
save "data/stkcd_anno_date.dta", replace

/* 600652 */
/* use "data/stkcd_anno_date.dta", clear
drop if date_anno==.
duplicates report date_anno stkcd
duplicates tag date_anno stkcd, gen(dup)
br if dup>0
br if stkcd ==600652 */

**************************************************
**************************************************
**# 2. preprocess market trading data
**************************************************
**************************************************
/*
label var markettype "5=综合A股,10=综合B股,15=综合AB股,21=综合A股和创业板,31=综合AB股和创业板"
Markettype [市场类型] - 1=上证A股市场 (不包含科创板），2=上证B股市场，4=深证A股市场（不包含创业板），8=深证B股市场，16=创业板， 32=科创板，64=北证A股市场。
*/
insheet using "$data_dir/日市场回报率91-23分市场/TRD_Dalym.csv", clear
// 考虑现金红利再投资 等权和流通市值加权 总市值加权
keep trddt markettype dretwdeq dretwdos dretwdtl
keep if inlist(markettype, 1, 4, 16, 32, 64)
rename dretwdeq mretew
rename dretwdos mretvw
rename dretwdtl mrettvw
label var mretew "Market Return (Equal Weighted)"
label var mretvw "Market Return (Outstanding Value Weighted)"
label var mrettvw "Market Return (Total Market Value Weighted)"
gen date_trade = date(trddt, "YMD")
format date_trade %td
drop trddt
label var date_trade "Trading Date"
reshape wide mretew mretvw mrettvw, i(date_trade) j(markettype)
foreach var in mretew mretvw mrettvw {
    rename `var'1 `var'_SH
    rename `var'4 `var'_SZ
    rename `var'16 `var'_GEM
    rename `var'32 `var'_STAR
    rename `var'64 `var'_BJ
}
save "data/market_return91-23.dta", replace

**************************************************
**************************************************
**# 3. Merge Closest Trading Date to Event Date
**************************************************
**************************************************
// Use individual return file instead of market file
/* use "data/market_return91-23.dta", clear
gen year_anno = year(date)
joinby year_anno using "data/stkcd_anno_date.dta", unmatched(both) _merge(_merge)
keep if _merge==3
drop _merge
gen date_diff_trade_anno = date - date_anno
keep if date_diff>=0
tab date_diff
// 保留最近的交易日
bys stkcd date_anno: egen min_date_diff_trade_anno = min(date_diff_trade_anno)
tab min_date_diff_trade_anno
keep if date_diff_trade_anno==min_date_diff_trade_anno
// 600652 重复一次
duplicates drop _all, force
// trade-anno 10 1家 11 2家 16 29家 19 14家
// 公告日不等于交易日的匹配最近的一个交易日
drop min_date_diff_trade_anno
rename date date_trade_to_anno
label var date_trade_to_anno "Closest Trading Date to Announcement Date"
drop date_anno year_anno
sort stkcd date_trade_to_anno
order stkcd date_trade_to_anno date_diff_trade_anno
gen year_trade = year(date_trade_to_anno)
keep stkcd date_trade_to_anno date_diff_trade_anno year_trade
save "data/stkcd_anno_trade_date.dta", replace */


**************************************************
**************************************************
**# 4. Individual Stock Return
**************************************************
**************************************************
capture program drop deal_one_return_file
program define deal_one_return_file
args infile outfile
insheet using "`infile'", clear
keep stkcd trddt dretwd markettype
keep if inlist(markettype, 1, 4, 16, 32, 64)
gen date_trade = date(trddt, "YMD")
format date_trade %td
drop trddt
label var date_trade "Trading Date"
label var dretwd "Daily Return with Dividend Reinvestment"
label var markettype "1 SZ 4 SH 16 GEM 32 STAR 64 BJ"
duplicates drop _all, force
save "`outfile'", replace
end

deal_one_return_file "$data_dir/日个股回报率04-08/TRD_Dalyr.csv" "data/stkcd_ret04-08.dta"
deal_one_return_file "$data_dir/日个股回报率04-08/TRD_Dalyr1.csv" "data/stkcd_ret04-08_1.dta"
deal_one_return_file "$data_dir/日个股回报率09-13/TRD_Dalyr.csv" "data/stkcd_ret09-13.dta"
deal_one_return_file "$data_dir/日个股回报率09-13/TRD_Dalyr1.csv" "data/stkcd_ret09-13_1.dta"
deal_one_return_file "$data_dir/日个股回报率09-13/TRD_Dalyr2.csv" "data/stkcd_ret09-13_2.dta"
deal_one_return_file "$data_dir/日个股回报率14-18/TRD_Dalyr.csv" "data/stkcd_ret14-18.dta"
deal_one_return_file "$data_dir/日个股回报率14-18/TRD_Dalyr1.csv" "data/stkcd_ret14-18_1.dta"
deal_one_return_file "$data_dir/日个股回报率14-18/TRD_Dalyr2.csv" "data/stkcd_ret14-18_2.dta"
deal_one_return_file "$data_dir/日个股回报率14-18/TRD_Dalyr3.csv" "data/stkcd_ret14-18_3.dta"
deal_one_return_file "$data_dir/日个股回报率19-23/TRD_Dalyr.csv" "data/stkcd_ret19-23.dta"
deal_one_return_file "$data_dir/日个股回报率19-23/TRD_Dalyr1.csv" "data/stkcd_ret19-23_1.dta"
deal_one_return_file "$data_dir/日个股回报率19-23/TRD_Dalyr2.csv" "data/stkcd_ret19-23_2.dta"
deal_one_return_file "$data_dir/日个股回报率19-23/TRD_Dalyr3.csv" "data/stkcd_ret19-23_3.dta"
deal_one_return_file "$data_dir/日个股回报率19-23/TRD_Dalyr4.csv" "data/stkcd_ret19-23_4.dta"
deal_one_return_file "$data_dir/日个股回报率19-23/TRD_Dalyr5.csv" "data/stkcd_ret19-23_5.dta"
/* insheet using "$data_dir/日个股回报率19-23/TRD_Dalyr5.csv", clear */
/* tab markettype */

// 这里使用09-23的数据
use "data/stkcd_ret09-13.dta", clear
append using "data/stkcd_ret09-13_1.dta"
append using "data/stkcd_ret09-13_2.dta"
append using "data/stkcd_ret14-18.dta"
append using "data/stkcd_ret14-18_1.dta"
append using "data/stkcd_ret14-18_2.dta"
append using "data/stkcd_ret14-18_3.dta"
append using "data/stkcd_ret19-23.dta"
append using "data/stkcd_ret19-23_1.dta"
append using "data/stkcd_ret19-23_2.dta"
append using "data/stkcd_ret19-23_3.dta"
append using "data/stkcd_ret19-23_4.dta"
append using "data/stkcd_ret19-23_5.dta"
save "data/stkcd_ret09-23.dta", replace
**************************************************
**************************************************
**# 5. Merge Event Date and Trading Date
**************************************************
**************************************************
capture program drop prepare_car_data
program define prepare_car_data
args infile outfile year_acc pre_esitmation post_estimation post_event least_points
use "data/`infile'", clear
keep if year_acc == `year_acc'  // 会计年度 用于区分事件
merge 1:m stkcd using "data/stkcd_ret09-23.dta", gen(_merge)
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
merge 1:m stkcd using "data/stkcd_ret09-23.dta", gen(_merge)
keep if _merge==3
drop _merge
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
save "data/ready_for_calculation/`outfile'", replace
end

// follow Liu Shu Wei JFE
cap: mkdir "data/ready_for_calculation"
local infile "stkcd_anno_date.dta"
prepare_car_data `infile' "event2010.dta" 2010 -365 -31 10 100
prepare_car_data `infile' "event2011.dta" 2011 -365 -31 10 100
prepare_car_data `infile' "event2012.dta" 2012 -365 -31 10 100
prepare_car_data `infile' "event2013.dta" 2013 -365 -31 10 100
prepare_car_data `infile' "event2014.dta" 2014 -365 -31 10 100
prepare_car_data `infile' "event2015.dta" 2015 -365 -31 10 100
prepare_car_data `infile' "event2016.dta" 2016 -365 -31 10 100
prepare_car_data `infile' "event2017.dta" 2017 -365 -31 10 100
prepare_car_data `infile' "event2018.dta" 2018 -365 -31 10 100
prepare_car_data `infile' "event2019.dta" 2019 -365 -31 10 100
prepare_car_data `infile' "event2020.dta" 2020 -365 -31 10 100
prepare_car_data `infile' "event2021.dta" 2021 -365 -31 10 100
prepare_car_data `infile' "event2022.dta" 2022 -365 -31 10 100
/* prepare_car_data `infile' "event2023.dta" 2023 -200 -10 5 100 */