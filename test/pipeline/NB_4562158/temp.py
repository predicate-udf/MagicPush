import os
import sys
cats = ["Finance",'Biotechnology','Health Care','E-Commerce','Software','Internet','Information Technology','Education','Security','Education','Real Estate','Tourism','Artificial Intelligence','Food','Advertising','Fashion','Data','Robotics','Gaming','Sports','Entertainment','Insurance']
slist = []
for c in cats:
    slist.append("'{}' if '{}' in xxx__['Category Groups'] else \\ \n".format(c, c)+"({})")
slist.append("'Unknown'")
s = slist[-1]
print(slist)
for i in reversed(range(len(slist)-1)):
    s = slist[i].format(s)
print(s)
