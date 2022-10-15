import pickle
import numpy as np
import pandas as pd
df0 = pickle.load(open("data_0.pickle",'rb'))
df0.columns = [str(c) for c in df0.columns]
df0["index"] = df0.index
size__before = df0.shape[0]

df0 = df0[df0.apply(lambda row: (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
print(len(df0))
size__after = df0.shape[0]
print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
df0_0 = df0[df0.apply(lambda row: (row['hotel_id'] == 21) and (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52) or (row['hotel_id'] != 21) and (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
df1 = df0_0.copy()
df1_0 = df1[df1.apply(lambda row: (row['hotel_id'] == 21) and (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
df1_1 = df1[df1.apply(lambda row: (row['hotel_id'] != 21) and (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
df2 = df1_0[df1_0.apply(lambda row: row['hotel_id'] == 21, axis=1)]
df2_0 = df2[df2.apply(lambda row: (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
df3 = df1_1[df1_1.apply(lambda row: row['hotel_id'] != 21, axis=1)]
df3_0 = df3[df3.apply(lambda row: (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
df4 = pd.concat([df2_0,df3_0], axis=0)
df4_0 = df4[df4.apply(lambda row: (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['distance']*row['distance']) == 0.23544780819273414 and (row['index'] == 52), axis=1)]
df5 = df4_0
print(df5)
df5["similarity_distance"] = df5.apply(lambda xxx__:xxx__['distance']*xxx__['distance'], axis=1)
df5_0 = df5[df5.apply(lambda row: (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['similarity_distance'] == 0.23544780819273414) and (row['index'] == 52), axis=1)]
df6 = df5_0.sort_values(by=["similarity_distance"])
df6_0 = df6[df6.apply(lambda row: (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['similarity_distance'] == 0.23544780819273414) and (row['index'] == 52), axis=1)]
df7 = df6_0.sort_values(by=["index"])
df7_0 = df7[df7.apply(lambda row: (row['hotel_id'] == 52.0) and (row['avg_rate'] == 179.0) and (row['distance'] == 0.48522964479999997) and (row['star_rating'] == 3.5) and (row['user_rating'] == np.nan) and (row['lat'] == 40.70580138) and (row['lng'] == -74.00509948) and (row['index'] == 52.0) and (row['similarity_distance'] == 0.23544780819273414) and (row['index'] == 52), axis=1)]
pickle.dump(df7, open('temp//result_after_pushdown.p', 'wb'))


# df0 = pickle.load(open("data_0.pickle",'rb'))
# df0.columns = [str(c) for c in df0.columns]
# df0["index"] = df0.index
# size__before = df0.shape[0]
# df0 = df0[df0.apply(lambda row: (row['hotel_id'] == 21) and (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29) or (row['hotel_id'] != 21) and (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# size__after = df0.shape[0]
# print("Input data_0.pickle filter reduce data from {} rows to {} rows".format(size__before, size__after))
# df0_0 = df0[df0.apply(lambda row: (row['hotel_id'] == 21) and (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29) or (row['hotel_id'] != 21) and (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# print(len(df0))
# df1 = df0_0.copy()
# df1_0 = df1[df1.apply(lambda row: (row['hotel_id'] == 21) and (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# df1_1 = df1[df1.apply(lambda row: (row['hotel_id'] != 21) and (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# df2 = df1_0[df1_0.apply(lambda row: row['hotel_id'] == 21, axis=1)]
# df2_0 = df2[df2.apply(lambda row: (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# df3 = df1_1[df1_1.apply(lambda row: row['hotel_id'] != 21, axis=1)]
# df3_0 = df3[df3.apply(lambda row: (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# df4 = pd.concat([df2_0,df3_0], axis=0)
# df4_0 = df4[df4.apply(lambda row: (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['distance']*row['distance']) == 0.37528647671682863 and (row['index'] == 29), axis=1)]
# df5 = df4_0
# df5["similarity_distance"] = df5.apply(lambda xxx__:xxx__['distance']*xxx__['distance'], axis=1)
# df5_0 = df5[df5.apply(lambda row: (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['similarity_distance'] == 0.37528647671682863) and (row['index'] == 29), axis=1)]
# df6 = df5_0.sort_values(by=["similarity_distance"])
# df6_0 = df6[df6.apply(lambda row: (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['similarity_distance'] == 0.37528647671682863) and (row['index'] == 29), axis=1)]
# df7 = df6_0.sort_values(by=["index"])
# df7_0 = df7[df7.apply(lambda row: (row['hotel_id'] == 29.0) and (row['avg_rate'] == 120.98) and (row['distance'] == 0.6126062983) and (row['star_rating'] == 3.0) and (row['user_rating'] == 4.0) and (row['lat'] == 40.719865000000006) and (row['lng'] == -73.99893399999999) and (row['index'] == 29.0) and (row['similarity_distance'] == 0.37528647671682863) and (row['index'] == 29), axis=1)]
# pickle.dump(df7, open('temp//result_after_pushdown.p', 'wb'))