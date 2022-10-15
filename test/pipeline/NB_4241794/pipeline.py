
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = DropNA(op0, ["koi_disposition","koi_fpflag_nt","koi_fpflag_ss","koi_fpflag_co","koi_fpflag_ec","koi_period","koi_period_err1","koi_period_err2","koi_time0bk","koi_time0bk_err1","koi_time0bk_err2","koi_impact","koi_impact_err1","koi_impact_err2","koi_duration","koi_duration_err1","koi_duration_err2","koi_depth","koi_depth_err1","koi_depth_err2","koi_prad","koi_prad_err1","koi_prad_err2","koi_teq","koi_insol","koi_insol_err1","koi_insol_err2","koi_model_snr","koi_tce_plnt_num","koi_steff","koi_steff_err1","koi_steff_err2","koi_slogg","koi_slogg_err1","koi_slogg_err2","koi_srad","koi_srad_err1","koi_srad_err2","ra","dec","koi_kepmag"])
op2 = DropNA(op1, ["koi_disposition","koi_fpflag_nt","koi_fpflag_ss","koi_fpflag_co","koi_fpflag_ec","koi_period","koi_period_err1","koi_period_err2","koi_time0bk","koi_time0bk_err1","koi_time0bk_err2","koi_impact","koi_impact_err1","koi_impact_err2","koi_duration","koi_duration_err1","koi_duration_err2","koi_depth","koi_depth_err1","koi_depth_err2","koi_prad","koi_prad_err1","koi_prad_err2","koi_teq","koi_insol","koi_insol_err1","koi_insol_err2","koi_model_snr","koi_tce_plnt_num","koi_steff","koi_steff_err1","koi_steff_err2","koi_slogg","koi_slogg_err1","koi_slogg_err2","koi_srad","koi_srad_err1","koi_srad_err2","ra","dec","koi_kepmag"])
op3 = DropColumns(op2, ["koi_fpflag_nt","koi_fpflag_ss","koi_fpflag_co","koi_fpflag_ec"])
op4 = DropColumns(op3, ["koi_period_err1","koi_period_err2","koi_time0bk_err1","koi_time0bk_err2","koi_impact_err1","koi_impact_err2","koi_duration_err1","koi_duration_err2","koi_depth_err1","koi_depth_err2","koi_prad_err1","koi_prad_err2","koi_insol_err1","koi_insol_err2","koi_steff_err1","koi_steff_err2","koi_slogg_err1","koi_slogg_err2","koi_srad_err1","koi_srad_err2","koi_tce_plnt_num"])
op6 = Filter(op4, Or(BinOp(Field("koi_disposition"),'==',Constant("CONFIRMED")),BinOp(Field("koi_disposition"),'==',Constant("FALSE POSITIVE"))))

