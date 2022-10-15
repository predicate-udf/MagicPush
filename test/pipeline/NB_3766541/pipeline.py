
import sys
import numpy as np
from ppl_interface import *

op0 = InitTable("data_0.pickle")
op1 = DropNA(op0, ["Strain","Type","Rating","Effects","Flavor","Description"])
op2 = ChangeType(op1, 'str', 'Effects','Effects')
op3 = ChangeType(op2, 'str', 'Type', 'Type')
op4 = GetDummies(op3, 'Effects', {'Aroused': 'str', 'Creative': 'str', 'Dry': 'str', 'Energetic': 'str', 'Euphoric': 'str', 'Focused': 'str', 'Giggly': 'str', 'Happy': 'str', 'Hungry': 'str', 'Mouth': 'str', 'None': 'str', 'Relaxed': 'str', 'Sleepy': 'str', 'Talkative': 'str', 'Tingly': 'str', 'Uplifted': 'str'})
op5 = GetDummies(op3, 'Type', {'hybrid': 'str', 'indica': 'str', 'sativa': 'str'})
op6 = DropColumns(op4, ["None"])
op7 = InnerJoin(op1, op6, ['index'], ['index'])
op8 = GetDummies(op3, 'Flavor', {'Ammonia': 'str', 'Apple': 'str', 'Apricot': 'str', 'Berry': 'str', 'Blue': 'str', 'Blueberry': 'str', 'Butter': 'str', 'Cheese': 'str', 'Chemical': 'str', 'Chestnut': 'str', 'Citrus': 'str', 'Coffee': 'str', 'Diesel': 'str', 'Earthy': 'str', 'Flowery': 'str', 'Fruit': 'str', 'Grape': 'str', 'Grapefruit': 'str', 'Honey': 'str', 'Lavender': 'str', 'Lemon': 'str', 'Lime': 'str', 'Mango': 'str', 'Menthol': 'str', 'Mint': 'str', 'Minty': 'str', 'None': 'str', 'Nutty': 'str', 'Orange': 'str', 'Peach': 'str', 'Pear': 'str', 'Pepper': 'str', 'Pine': 'str', 'Pineapple': 'str', 'Plum': 'str', 'Pungent': 'str', 'Rose': 'str', 'Sage': 'str', 'Skunk': 'str', 'Spicy/Herbal': 'str', 'Strawberry': 'str', 'Sweet': 'str', 'Tar': 'str', 'Tea': 'str', 'Tobacco': 'str', 'Tree': 'str', 'Tropical': 'str', 'Vanilla': 'str', 'Violet': 'str', 'Woody': 'str'})
op9 = DropColumns(op8, ["None"])
op10 = InnerJoin(op7, op9, ['index'], ['index'])
op11 = InnerJoin(op10, op5, ['index'], ['index'])
op12 = DropColumns(op11, ["Effects","Flavor","Type","Strain","Description"])
