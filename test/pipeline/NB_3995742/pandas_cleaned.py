import pandas as pd
import pickle
import numpy as np
df0=pickle.load(open('data_0.pickle','rb'))
df3 = df0[df0["COLPROT"] == "ADNI2"]
df4 = df3.replace(to_replace={ "Unknown":np.nan })
df5 = df4.replace(to_replace={ -1:np.nan })
df6 = df5.replace(to_replace={ -4:np.nan })
df7 = df6.copy()
df4 = df7.drop(columns=["Month_bl","Month","M"])
df10 = df4[df4["DX"].notnull()]
df16 = df10.drop(columns=["PIB_bl","PIB","AV45","FLDSTRENG","MidTemp","Entorhinal","Fusiform","FDG"])
df29 = df16.copy()
df29.dropna(subset=["RID","PTID","VISCODE","SITE","COLPROT","ORIGPROT","EXAMDATE","DX_bl","AGE","PTGENDER","PTEDUCAT","PTETHCAT","PTRACCAT","PTMARRY","APOE4","CDRSB","ADAS11","ADAS13","MMSE","RAVLT_immediate","RAVLT_learning","RAVLT_forgetting","RAVLT_perc_forgetting","FAQ","MOCA","EcogPtMem","EcogPtLang","EcogPtVisspat","EcogPtPlan","EcogPtOrgan","EcogPtDivatt","EcogPtTotal","EcogSPMem","EcogSPLang","EcogSPVisspat","EcogSPPlan","EcogSPOrgan","EcogSPDivatt","EcogSPTotal","FSVERSION","Ventricles","Hippocampus","WholeBrain","ICV","DX","EXAMDATE_bl","CDRSB_bl","ADAS11_bl","ADAS13_bl","MMSE_bl","RAVLT_immediate_bl","RAVLT_learning_bl","RAVLT_forgetting_bl","RAVLT_perc_forgetting_bl","FAQ_bl","FLDSTRENG_bl","FSVERSION_bl","Ventricles_bl","Hippocampus_bl","WholeBrain_bl","Entorhinal_bl","Fusiform_bl","MidTemp_bl","ICV_bl","MOCA_bl","EcogPtMem_bl","EcogPtLang_bl","EcogPtVisspat_bl","EcogPtPlan_bl","EcogPtOrgan_bl","EcogPtDivatt_bl","EcogPtTotal_bl","EcogSPMem_bl","EcogSPLang_bl","EcogSPVisspat_bl","EcogSPPlan_bl","EcogSPOrgan_bl","EcogSPDivatt_bl","EcogSPTotal_bl","FDG_bl","AV45_bl","Years_bl","update_stamp"], inplace=True)


