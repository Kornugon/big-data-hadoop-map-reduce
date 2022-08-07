import re

"""
\w = a-z & A-Z & 0-9 & _
[\w]+   znaczy ze szukamy conajmniej jednego takiego elementu
"""

WORD_RE = re.compile(r'[\w]+')

words = WORD_RE.findall('Big data, hadoop and map reduce. (Hello world!')
print(words)