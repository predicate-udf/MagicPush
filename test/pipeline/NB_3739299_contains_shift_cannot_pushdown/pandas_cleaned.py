NB_3739299.py: hourly_for_group /      return ridership_df.groupby('UNIT')['ENTRIESn', 'EXITSn'].apply(hourly_for_group)

ridership_df = pd.DataFrame({

    'UNIT': ['R051', 'R079', 'R051', 'R079', 'R051', 'R079', 'R051', 'R079', 'R051'],

    'TIMEn': ['00:00:00', '02:00:00', '04:00:00', '06:00:00', '08:00:00', '10:00:00', '12:00:00', '14:00:00', '16:00:00'],

    'ENTRIESn': [3144312, 8936644, 3144335, 8936658, 3144353, 8936687, 3144424, 8936819, 3144594],

    'EXITSn': [1088151, 13755385,  1088159, 13755393,  1088177, 13755598, 1088231, 13756191,  1088275]

})
def hourly_for_group(entries_and_exits):
    return entries_and_exits - entries_and_exits.shift(1)
ridership_df.groupby('UNIT')['ENTRIESn', 'EXITSn'].apply(hourly_for_group)
