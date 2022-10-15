import pickle
df0 = pickle.load(open("pipeline1_data/phoenix_business_ws_rw_ffall_merged2.p",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
df1 = pickle.load(open("pipeline1_data/arizon.p",'rb'))
df1.columns = [str(c) for c in df1.columns]
df1["index"] = df1.index
df2 = df0[(df0['ffall'] < 888) & (df0['ffall'] >= 3)]
df3 = df2
df3["totalStars"] = df3.apply(lambda x: x['review_count']*x['stars'], axis=1)
df4 = df3
df4["adjwhp"] = df4.apply(lambda x: x['white_pop']*x['stars'], axis=1)
df5 = df4.groupby(["zipcode"]).agg({ "review_count":"mean","ffall":"mean","ffall_category":"mean" }).reset_index().rename(columns={ "review_count":"avgrc","ffall":"avgffall","ffall_category":"avgffc" })
df6 = df5.drop_duplicates(subset=["zipcode","avgrc","avgffall","avgffc"])
df7 = df4.merge(df1, left_on = ["zipcode"], right_on = ["zipcode"])
df8 = df6.merge(df7, left_on = ["zipcode"], right_on = ["zipcode"])
final_output = df8[(df8['zipcode'] == 85054) & (df8['avgrc'] == 108.69047619047619) & (df8['avgffall'] == 252.33333333333334) & (df8['avgffc'] == 2.142857142857143) & (df8['business_id'] == 'zKRAIVLNmtLsiqjYT2XAQw') & (df8['Mexican'] == 0) & (df8['American (Traditional)'] == 0) & (df8['Pizza'] == 0) & (df8['American (New)'] == 1) & (df8['Burgers'] == 1) & (df8['Italian'] == 0) & (df8['Chinese'] == 0) & (df8['Salad'] == 0) & (df8['Sports Bars'] == 0) & (df8['Seafood'] == 0) & (df8['Japanese'] == 0) & (df8['Barbeque'] == 0) & (df8['Mediterranean'] == 0) & (df8['Sushi Bars'] == 0) & (df8['Asian Fusion'] == 0) & (df8['Steakhouses'] == 0) & (df8['Greek'] == 0) & (df8['Tex-Mex'] == 0) & (df8['Thai'] == 0) & (df8['Vietnamese'] == 0) & (df8['Indian'] == 0) & (df8['Middle Eastern'] == 0) & (df8['Southern'] == 0) & (df8['Latin American'] == 0) & (df8['Hawaiian'] == 0) & (df8['Korean'] == 0) & (df8['French'] == 0) & (df8['Caribbean'] == 0) & (df8['Pakistani'] == 0) & (df8['Ramen'] == 0) & (df8['New Mexican Cuisine'] == 0) & (df8['Modern European'] == 0) & (df8['Spanish'] == 0) & (df8['African'] == 0) & (df8['Cantonese'] == 0) & (df8['Persian/Iranian'] == 0) & (df8['Filipino'] == 0) & (df8['Cuban'] == 0) & (df8['Mongolian'] == 0) & (df8['Lebanese'] == 0) & (df8['Polish'] == 0) & (df8['Taiwanese'] == 0) & (df8['German'] == 0) & (df8['Turkish'] == 0) & (df8['Ethiopian'] == 0) & (df8['Brazilian'] == 0) & (df8['Afghan'] == 0) & (df8['zipcode.1'] == 85054) & (df8['total_pop_x'] == 5384) & (df8['occupied_housing_units_x'] == 2967) & (df8['white_pop_x'] == 4636) & (df8['afam_pop_x'] == 173) & (df8['amindian_pop_x'] == 34) & (df8['asian_pop_x'] == 293) & (df8['hawaiian_pop_x'] == 5) & (df8['other_race_x'] == 104) & (df8['male_x'] == 2663) & (df8['female_x'] == 2721) & (df8['median_income_x'] == 76413) & (df8['median_age_x'] == 35.2) & (df8['under_18_x'] == 738) & (df8['above_18_x'] == 4633) & (df8['walkscore'] == 14.0) & (df8['ffall'] == 214) & (df8['stars'] == 4.0) & (df8['review_count'] == 98) & (df8['ffall_category'] == 2) & (df8['CuisineCombined'] == 9) & (df8['index_x'] == 958) & (df8['totalStars'] == 392.0) & (df8['adjwhp'] == 18544.0) & (df8['Unnamed: 0'] == 66) & (df8['Unnamed: 0.1'] == 66.0) & (df8['median_income_y'] == 76413.0) & (df8['housing_units'] == 3801.0) & (df8['occupied_housing_units_y'] == 2967.0) & (df8['total_pop_y'] == 5384.0) & (df8['white_pop_y'] == 4636.0) & (df8['afam_pop_y'] == 173.0) & (df8['amindian_pop_y'] == 34.0) & (df8['asian_pop_y'] == 293.0) & (df8['hawaiian_pop_y'] == 5.0) & (df8['other_race_y'] == 104.0) & (df8['two_or_more'] == 139.0) & (df8['male_y'] == 2663.0) & (df8['female_y'] == 2721.0) & (df8['median_age_y'] == 35.2) & (df8['under_18_y'] == 738.0) & (df8['above_18_y'] == 4633.0) & (df8['PCT0050002'] == 113.0) & (df8['PCT0050003'] == 0.0) & (df8['PCT0050004'] == 0.0) & (df8['PCT0050005'] == 0.0) & (df8['PCT0050006'] == 0.0) & (df8['PCT0050007'] == 43.0) & (df8['PCT0050008'] == 50.0) & (df8['PCT0050009'] == 0.0) & (df8['PCT0050010'] == 1.0) & (df8['PCT0050011'] == 20.0) & (df8['PCT0050012'] == 13.0) & (df8['PCT0050013'] == 1.0) & (df8['PCT0050014'] == 0.0) & (df8['PCT0050015'] == 0.0) & (df8['PCT0050016'] == 9.0) & (df8['PCT0050017'] == 1.0) & (df8['PCT0050018'] == 6.0) & (df8['PCT0050019'] == 1.0) & (df8['PCT0050020'] == 17.0) & (df8['PCT0050021'] == 0.0) & (df8['PCT0050022'] == 8.0) & (df8['state'] == 4.0) & (df8['zip'] == 85054) & (df8['naics'] == '------') & (df8['est'] == 248) & (df8['n1_4'] == 121) & (df8['n5_9'] == 28) & (df8['n10_19'] == 30) & (df8['n20_49'] == 32) & (df8['n50_99'] == 19) & (df8['n100_249'] == 11) & (df8['n250_499'] == 2) & (df8['n500_999'] == 2) & (df8['n1000'] == 3) & (df8['index_y'] == 66)]
print(final_output.columns)
pickle.dump(final_output, open('temp//result_pred_on_output.p', 'wb'))