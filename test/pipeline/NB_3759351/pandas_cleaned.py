import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df1 = df0.drop(columns=["Unnamed: 0"])
df8 = df1[df1["air_area_name"] != "Hokkaidō Katō-gun Motomachi"]
df11 = df8[df8["air_area_name"] != "Niigata-ken Kashiwazaki-shi Chūōchō"]
df14 = df11[df11["air_area_name"] != "Fukuoka-ken Fukuoka-shi Tenjin"]
df17 = df14[df14["air_area_name"] != "Tōkyō-to Meguro-ku Takaban"]
df20 = df17[df17["air_area_name"] != "Tōkyō-to Chiyoda-ku Kanda Jinbōchō"]
df22 = df20[df20["air_area_name"] != "Tōkyō-to Musashino-shi Midorichō"]
df25 = df22[df22["air_area_name"] != "Tōkyō-to Adachi-ku Chūōhonchō"]
df28 = df25[df25["air_area_name"] != "Tōkyō-to Kōtō-ku Tomioka"]
df31 = df28[df28["air_area_name"] != "Hokkaidō Sapporo-shi Atsubetsuchūō 1 Jō"]
df34 = df31[df31["air_area_name"] != "Tōkyō-to Edogawa-ku Chūō"]
df37 = df34[df34["air_area_name"] != "Hokkaidō Sapporo-shi Kita 24 Jōnishi"]
df40 = df37[df37["air_area_name"] != "Tōkyō-to Fuchū-shi Miyanishichō"]
df42 = df40[df40["air_area_name"] != "Ōsaka-fu Ōsaka-shi Ōhiraki"]
df45 = df42[df42["air_area_name"] != "Niigata-ken Niigata-shi Teraohigashi"]
df48 = df45[df45["air_area_name"] != "Ōsaka-fu Ōsaka-shi Nanbasennichimae"]
df51 = df48[df48["air_area_name"] != "Fukuoka-ken Kitakyūshū-shi Konyamachi"]
df54 = df51[df51["air_area_name"] != "Hyōgo-ken Amagasaki-shi Higashinanamatsuchō"]
df57 = df54[df54["air_area_name"] != "Tōkyō-to Musashino-shi Kichijōji Honchō"]
df60 = df57[df57["air_area_name"] != "Tōkyō-to Taitō-ku Asakusa"]
df63 = df60[df60["air_area_name"] != "Ōsaka-fu Suita-shi Izumichō"]
df66 = df63[df63["air_area_name"] != "Tōkyō-to Toshima-ku Sugamo"]
df69 = df66[df66["air_area_name"] != "Tōkyō-to Meguro-ku Jiyūgaoka"]
df72 = df69[df69["air_area_name"] != "Fukuoka-ken Fukuoka-shi Imaizumi"]
df75 = df72[df72["air_area_name"] != "Tōkyō-to Shibuya-ku Higashi"]
df78 = df75[df75["air_area_name"] != "Tōkyō-to Setagaya-ku Kitazawa"]
sr82 = df78.groupby(by=["current_week","day_of_week","air_area_name"])["visitors"].sum().reset_index()
