import sys

import pandas as pd


print(sys.argv)

#sys.argv[0] : Arg 0 is Name of the File

#We want ag=rg 1 which is whatever we pass in CLI

#Some fancy stuff with pandas
day = sys.argv[1]
print(f'Job finished successfully for day = {day}')
