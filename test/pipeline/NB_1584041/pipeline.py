import sys
sys.path.append("/datadrive/yin/predicate_pushdown_for_lineage_tracking/")
from interface import *

segments = InitTable("transit_segments.pickle")
vessels = InitTable("vessel_information.pickle")
segments_merged = InnerJoin(segments, vessels, 'index', 'mmsi')

sub_op0 = SubpipeInput(segments_merged, 'group', ['mmsi'])
sub_op1 = TopN(sub_op0, 3, ['seg_length'])
top3segments = CogroupedMap(SubPipeline(PipelinePath([sub_op0, sub_op1])))
top3segments1 = DropColumns(top3segments, ['num_names', 'sov', 'flag', 'flag_type', 'num_loas', 'loa', \
       'max_loa', 'num_types', 'type', 'mmsi', 'name', 'transit', 'segment', \
       'avg_sog', 'min_sog', 'max_sog', 'pdgt10', 'st_time', 'end_time'])
