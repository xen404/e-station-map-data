from datetime import datetime, timedelta
from tqdm import tqdm
import glob

dates = []
for path in tqdm(glob.glob('all_data/*')):
    timestamp_str = path.split('/')[-1][:-5]
    timestamp = int(timestamp_str)//1000
    dates.append(datetime.fromtimestamp(timestamp))
dates.sort()
counter = 0
for date1, date2 in zip(dates[1:], dates[:-1]):
    delta = date1-date2
    if delta>timedelta(minutes=15):
      print(f'{date1} {date2} delta: {delta}')
    counter += 1
