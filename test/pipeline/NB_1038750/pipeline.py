
import sys
sys.path.append("../../../")
sys.path.append("../../")
import z3
import dis
from interface import *
from util import *
import random
from predicate import *
from generate_input_filters import *
from compare_pushdown_result import get_output_filter, check_pushdown_result
import os



op0 = InitTable("data_0.pickle")
op3 = GroupBy(op0, ["Data","País/Região"], { "Latitude":(Value(0, True),"mean"),"Longitude":(Value(0, True),"mean"),"PIB 2019":(Value(0, False),"unique"),"População":(Value(0, False),"unique"),"População Urbana":(Value(0, False),"unique"),"Área (Km/2)":(Value(0, False),"unique"),"Densidade (Km/2)":(Value(0, False),"unique"),"Taxa de Fertilidade":(Value(0, True),"mean"),"Média de Idade":(Value(0, True),"mean"),"Taxa de Fumantes":(Value(0, True),"mean"),"Taxa de Mortalidade por Doenças Pulmonares":(Value(0, True),"mean"),"Total de Leitos Hospitalares":(Value(0, False),"min"),"Temperatura Média (Janeiro - Março)":(Value(0, True),"mean"),"Taxa de Umidade (Janeiro - Março)":(Value(0, True),"mean"),"Casos Confirmados":(Value(0, True),"sum") }, { "Latitude":"Latitude","Longitude":"Longitude","PIB 2019":"PIB 2019","População":"População","População Urbana":"População Urbana","Área (Km/2)":"Área (Km/2)","Densidade (Km/2)":"Densidade (Km/2)","Taxa de Fertilidade":"Taxa de Fertilidade","Média de Idade":"Média de Idade","Taxa de Fumantes":"Taxa de Fumantes","Taxa de Mortalidade por Doenças Pulmonares":"Taxa de Mortalidade por Doenças Pulmonares","Total de Leitos Hospitalares":"Total de Leitos Hospitalares","Temperatura Média (Janeiro - Março)":"Temperatura Média (Janeiro - Março)","Taxa de Umidade (Janeiro - Março)":"Taxa de Umidade (Janeiro - Março)","Casos Confirmados":"Casos Confirmados" })

op23 = SortValues(op3, ["Data","País/Região"])
op24 = DropDuplicate(op23, ["País/Região"])
op25 = DropColumns(op24, ["Data","Casos Confirmados"])
op28 = Pivot(op23, 'País/Região','Data','Casos Confirmados', None, {k:'int' for k in ['1/22/20', '1/23/20', '1/24/20', '1/25/20', '1/26/20', '1/27/20',
    '1/28/20', '1/29/20', '1/30/20', '1/31/20', '2/1/20', '2/10/20',
    '2/11/20', '2/12/20', '2/13/20', '2/14/20', '2/15/20', '2/16/20',
    '2/17/20', '2/18/20', '2/19/20', '2/2/20', '2/20/20', '2/21/20',
    '2/22/20', '2/23/20', '2/24/20', '2/25/20', '2/26/20', '2/27/20',
    '2/28/20', '2/29/20', '2/3/20', '2/4/20', '2/5/20', '2/6/20', '2/7/20',
    '2/8/20', '2/9/20', '3/1/20', '3/10/20', '3/11/20', '3/12/20',
    '3/13/20', '3/14/20', '3/15/20', '3/16/20', '3/17/20', '3/18/20',
    '3/19/20', '3/2/20', '3/20/20', '3/21/20', '3/22/20', '3/23/20',
    '3/24/20', '3/25/20', '3/26/20', '3/27/20', '3/28/20', '3/29/20',
    '3/3/20', '3/30/20', '3/31/20', '3/4/20', '3/5/20', '3/6/20', '3/7/20',
    '3/8/20', '3/9/20', '4/1/20', '4/10/20', '4/11/20', '4/12/20',
    '4/13/20', '4/14/20', '4/15/20', '4/2/20', '4/3/20', '4/4/20', '4/5/20',
    '4/6/20', '4/7/20', '4/8/20', '4/9/20']})


#op29 = SortValues(op28, ["index"])


op33 = LeftOuterJoin(op25, op28, ["País/Região"],["País/Região"])
op34 = Filter(op33, BinOp(Field("País/Região"), '==', Constant('Afghanistan')))
op35 = DropColumns(op34, ['PIB 2019', 'População',
       'Área (Km/2)', 'Densidade (Km/2)',
       'Taxa de Fertilidade', 'Média de Idade', 'Taxa de Fumantes',
       'Taxa de Mortalidade por Doenças Pulmonares',
       'Total de Leitos Hospitalares', 'Temperatura Média (Janeiro - Março)',
       'Taxa de Umidade (Janeiro - Março)', '1/22/20', '1/23/20', '1/24/20',
       '1/25/20', '1/26/20', '1/27/20', '1/28/20', '1/29/20', '1/30/20',
       '1/31/20', '2/1/20', '2/10/20', '2/11/20', '2/12/20', '2/13/20',
       '2/14/20', '2/15/20', '2/16/20', '2/17/20', '2/18/20', '2/19/20',
       '2/2/20', '2/20/20', '2/21/20', '2/22/20', '2/23/20', '2/24/20',
       '2/25/20', '2/26/20', '2/27/20', '2/28/20', '2/29/20', '2/3/20',
       '2/4/20', '2/5/20', '2/6/20', '2/7/20', '2/8/20', '2/9/20', '3/1/20',
       '3/10/20', '3/11/20', '3/12/20', '3/13/20', '3/14/20', '3/15/20',
       '3/16/20', '3/17/20', '3/18/20', '3/19/20', '3/2/20', '3/20/20',
       '3/21/20', '3/22/20', '3/23/20', '3/24/20', '3/25/20', '3/26/20',
       '3/27/20', '3/28/20', '3/29/20', '3/3/20', '3/30/20', '3/31/20',
       '3/4/20', '3/5/20', '3/6/20', '3/7/20', '3/8/20', '3/9/20', '4/1/20',
       '4/10/20', '4/11/20', '4/12/20', '4/13/20', '4/14/20', 
       '4/2/20', '4/3/20', '4/4/20', '4/5/20', '4/6/20', '4/7/20', '4/8/20',
       '4/9/20'])
ops = [op0,op3, op23, op24, op25, op28, op33]

output_schemas = generate_output_schemas(ops)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
    
mkdir('./temp')


output_filter = get_output_filter(ops, './temp')

print(output_filter)
#print(output_filter)
#print(output_filter)
#exit(0)
for op_id,op_i in reversed([(k1,v1) for k1,v1 in enumerate(ops)]):
    if(op_i == ops[-1]):
        output_filter_i = {None:output_filter}
    else:
        output_filter_i = generate_output_filter_from_previous(op_i, ops)
    output_filter_i = AllOr(*list(output_filter_i.values()))
    inference_i = op_i.get_inference_instance(output_filter_i)
    inference_i.input_filters = generate_input_filters(op_i, inference_i, output_filter_i)
    # print(output_filter_i)
    print(op_id, ':')
    print_input_filters(inference_i)
    #print(inference_i.output_filter)

check_pushdown_result(ops, 'temp/')
