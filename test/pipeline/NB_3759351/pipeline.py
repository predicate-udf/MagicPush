
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = DropColumns(op0, ["Unnamed: 0"])
op3 = Filter(op1, BinOp(Field("air_area_name"),'!=',Constant("Hokkaidō Katō-gun Motomachi")))
op4 = Filter(op3, BinOp(Field("air_area_name"),'!=',Constant("Niigata-ken Kashiwazaki-shi Chūōchō")))
op5 = Filter(op4, BinOp(Field("air_area_name"),'!=',Constant("Fukuoka-ken Fukuoka-shi Tenjin")))
op6 = Filter(op5, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Meguro-ku Takaban")))
op7 = Filter(op6, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Chiyoda-ku Kanda Jinbōchō")))
op8 = Filter(op7, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Musashino-shi Midorichō")))
op9 = Filter(op8, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Adachi-ku Chūōhonchō")))
op10 = Filter(op9, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Kōtō-ku Tomioka")))
op11 = Filter(op10, BinOp(Field("air_area_name"),'!=',Constant("Hokkaidō Sapporo-shi Atsubetsuchūō 1 Jō")))
op12 = Filter(op11, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Edogawa-ku Chūō")))
op13 = Filter(op12, BinOp(Field("air_area_name"),'!=',Constant("Hokkaidō Sapporo-shi Kita 24 Jōnishi")))
op14 = Filter(op13, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Fuchū-shi Miyanishichō")))
op15 = Filter(op14, BinOp(Field("air_area_name"),'!=',Constant("Ōsaka-fu Ōsaka-shi Ōhiraki")))
op16 = Filter(op15, BinOp(Field("air_area_name"),'!=',Constant("Niigata-ken Niigata-shi Teraohigashi")))
op17 = Filter(op16, BinOp(Field("air_area_name"),'!=',Constant("Ōsaka-fu Ōsaka-shi Nanbasennichimae")))
op18 = Filter(op17, BinOp(Field("air_area_name"),'!=',Constant("Fukuoka-ken Kitakyūshū-shi Konyamachi")))
op19 = Filter(op18, BinOp(Field("air_area_name"),'!=',Constant("Hyōgo-ken Amagasaki-shi Higashinanamatsuchō")))
op20 = Filter(op19, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Musashino-shi Kichijōji Honchō")))
op21 = Filter(op20, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Taitō-ku Asakusa")))
op22 = Filter(op21, BinOp(Field("air_area_name"),'!=',Constant("Ōsaka-fu Suita-shi Izumichō")))
op23 = Filter(op22, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Toshima-ku Sugamo")))
op24 = Filter(op23, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Meguro-ku Jiyūgaoka")))
op25 = Filter(op24, BinOp(Field("air_area_name"),'!=',Constant("Fukuoka-ken Fukuoka-shi Imaizumi")))
op26 = Filter(op25, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Shibuya-ku Higashi")))
op27 = Filter(op26, BinOp(Field("air_area_name"),'!=',Constant("Tōkyō-to Setagaya-ku Kitazawa")))
op28 = GroupBy(op27, ["current_week","day_of_week","air_area_name"], { "visitors":(Value(0, True),"sum") }, { "visitors":"visitors" })
