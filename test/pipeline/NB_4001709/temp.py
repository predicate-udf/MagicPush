import os
import sys

states = """
Alabama	AL	Ala.
Alaska	AK	Alaska
Arizona	AZ	Ariz.
Arkansas	AR	Ark.
California	CA	Calif.
Colorado	CO	Color.
Connecticut	CT	Conn.
Delaware	DE	Del.
Florida	FL	Fla.
Georgia	GA	Ga.
Hawaii	HI	Hawaii
Idaho	ID	Idaho
Illinois	IL	Ill.
Indiana	IN	Ind.
Iowa	IA	Iowa
Kansas	KS	Kan.
Kentucky	KY	Ky.
Louisiana	LA	La.
Maine	ME	Maine
Maryland	MD	Md.
Massachusetts	MA	Mass.
Michigan	MI	Mich.
Minnesota	MN	Minn.
Mississippi	MS	Miss.
Missouri	MO	Mo.
Montana	MT	Mont.
Nebraska	NE	Neb.
Nevada	NV	Nev.
New Hampshire	NH	N.H.
New Jersey	NJ	N.J.
New Mexico	NM	N.M.
New York	NY	N.Y.
North Carolina	NC	N.C.
North Dakota	ND	N.D.
Ohio	OH	Ohio
Oklahoma	OK	Okla.
Oregon	OR	Ore.
Pennsylvania	PA	Pa.
Rhode Island	RI	R.I.
South Carolina	SC	S.C.
South Dakota	SD	S.Dak.
Tennessee	TN	Tenn.
Texas	TX	Tex.
Utah	UT	Utah
Vermont	VT	V.T.
Virginia	VA	Va.
Washington	WA	Wash.
West Virginia	WV	W.Va.
Wisconsin	WI	Wis.
Wyoming	WY	Wyo.
"""
m = {}
for line in states.split('\n'):
    if len(line) == 0:
        continue
    chs = line.split('\t')
    m[chs[1].upper()] = chs[0].upper()
print(m)
print([k for k,v in m.items()])
