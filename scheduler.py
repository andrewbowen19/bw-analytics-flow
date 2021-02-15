# Scheduling script to run flow_run daily

import os
import time
import schedule
from flow_run import *

print('Setting up schedule...')
# Episodes included with podcast level func
schedule.every().day.at("11:30").do(update_network_level)
schedule.every().day.at("11:30").do(update_podcast_level)

print('Schedule created!')                         