
import argparse
import os
import importlib

import logstar_stream.processing_steps.ProcessingStep as plg

IGNORE_IN_PATH = ["__init__.py", "__pycache__"]


def main():
    parser = argparse.ArgumentParser(description='''
    Manager to show available processing steps in LogstarOnlineStream 
    
    Processing steps can be used to manipulate data after it got downloaded with logstar-online-stream. So the data on 
    csv-files or the database is changed by the given processing step. This can be used to clean the data, remove outliers 
    and so on.

    Processing steps have to be placed inside {PLUGINS_DIR} directory and must inherit from LogstarOnlineStreamProcessingStep in processing_step.py
    '''
                                     )
    parser.add_argument("-l", "--list", required=False, dest="list", action='store_true',
                        help="list available plugins in plugins dir")
    args = parser.parse_args()
    if args.list:
        plugins = os.listdir(plg.PS_DIR)
        for plugin in plugins:
            if plugin in IGNORE_IN_PATH:
                continue
            class_name = plugin.split(".")[0]
            module = plg.PS_MOD_DIR + class_name
            print("loading {} as module: {} ...".format(plugin, module))
            m = importlib.import_module(module)
            c = eval("m.{}".format(class_name))

            process = getattr(c, "process", None)
            if callable(process):
                print("Class: {} checked ...".format(c.ps_name))
            else:
                print("Class: {} not valid ...".format(c.ps_name))


if __name__ == '__main__':
    main()
