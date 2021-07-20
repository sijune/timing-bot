#!/usr/bin/env python3

import os, sys
from pyrunner import PyRunner

if __name__ == '__main__':
  # Determine absolute path of this file's parent directory at runtime
  abs_dir_path = os.path.dirname(os.path.realpath(__file__))
  
  # Store path to default config and .lst file
  config_file = '{}/config/app_profile'.format(abs_dir_path)
  proc_file = '{}/config/timing-bot-runner.lst'.format(abs_dir_path)
  
  # Init PyRunner and assign default config and .lst file
  app = PyRunner(config_file=config_file, proc_file=proc_file)
  
  # Initiate job and exit driver with return code
  sys.exit(app.execute())
