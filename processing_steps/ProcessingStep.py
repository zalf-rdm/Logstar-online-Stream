
import sys
import importlib
from typing import List, Dict
import logging

import pandas as pd

PS_DIR = "processing_steps/"
PS_MOD_DIR = PS_DIR.replace("/", ".")

PS_LOGGING_DIR = "/processing-logs/"

def load_class(p):
  """
  createing a processing step object of a string like: "PSNAME arg1=value1 arg2=value2"
  checks if the PSNAME is a class name in processing_steps folder.
  The class name must have the same name as the file in the ProcessingStep folder 

  param: p creation string
  
  return object instance
  """

  class_name = p[0]
  args =  {}
  for arg_k_v in p[1:]:
    if "=" in arg_k_v:
      k,v = arg_k_v.split("=")
      args[k] = v
    else:
      logging.error("Processing step args require an assignment operator(=). Like key=value ...")
      sys.exit(1)

  module = PS_MOD_DIR + class_name
  logging.info(f"loading {class_name} with args {args} as module: {module} ...")
  m = importlib.import_module(module)
  c = eval("m.{}".format(class_name))
  return c(args=args)

class ProcessingStep(object):

    ps_name = "AbstractLogstarOnlineStreamProcessingStep"

    def __init__(self, args: Dict):
        pass

    def process(self, df: pd.DataFrame, station_name: str):
        ''' processes data and may manipulates it '''
        pass
