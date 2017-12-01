##############################################################################
#  Copyright (c) 2017 Patrick Kutch
# 
# Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##############################################################################
#    File Abstract: 
#   Application that merges, modifies BIFF safe files from Oscar.
#
##############################################################################
import argparse
import os
import sys
import logging
import pickle
import xml.dom.minidom
from xml.parsers.expat import ExpatError

from Helpers import Log
from Helpers import FileHandler
from Helpers import VersionMgr


def existFile(filename):
    if not os.path.exists(filename):
        Log.getLogger().error("Specified file: " + str(filename) + " does not exist.")
        return False
    return True


def ReadConfigFile(fileName,outfile):
    if not existFile(fileName):
        return False
        
    #open the xml file for reading:
    file = open(fileName,'r')
    #convert to string:
    data = file.read()
    #close file because we dont need it anymore:
    file.close()
    inputList=[]
    try:
        domDoc = xml.dom.minidom.parseString(data)

        sourceList = domDoc.getElementsByTagName('Source')

        # run through quickly and verify input files are specified and exist
        for source in sourceList:
            if "File" in source.attributes:
                sourceFile = source.attributes["File"].nodeValue
                if not existFile(sourceFile):
                    return False
                
            else:
                Log.getLogger().error("No File specified for source")

        resultList=[]
        appendList=[]

        lastTime = 0
        for source in sourceList:
            fHandler = FileHandler.FileHandler(source)
            if fHandler.insertTime == "Append":
                appendList.append(fHandler)
            else:
                   resultList = FileHandler.mergeLists(resultList,fHandler.createMergedList())

        if len(resultList) > 0:
            lastTime = resultList[-1].ArrivalTime

        for fHandler in appendList:
                resultList = FileHandler.mergeLists(resultList,fHandler.createMergedList(lastTime + 1))
                lastTime = resultList[-1].ArrivalTime
            
        try:
            with open(outfile,'w+b') as fp:
               pickle.dump(resultList, fp, pickle.DEFAULT_PROTOCOL)

            print("New file [" + outfile + "] created with " + str(len(resultList)) + " entries.")
        except Exception as ex:
            print(str(ex))
            return False



    except pickle.UnpicklingError:
        return False

    except Exception as ex:
        Log.getLogger().error("Bad Content - XML error: " + str(ex))
        return False

    return True

def main():
    if not HandleCommandlineArguments():
        return

def HandleCommandlineArguments():
    print("FUDD - BIFF Save File Editor Version " + VersionMgr.ReadVer())

    if sys.version_info < (3, 3):
        print("---- Error: Required Python 3.3 or greater ---")
        return False
    
    parser = argparse.ArgumentParser(description='FUDD the fearful')

    parser.add_argument("-i","--input",help='specifies application configuration file file',type=str,required=True)
    parser.add_argument("-o","--output",help='specifies file to generate',type=str,required=True)
    parser.add_argument("-l","--logfile",help='specifies log file name',type=str)
    parser.add_argument("-v","--verbose",help="prints debug information",action="store_true")

    try:    
        args = parser.parse_args()

    except:
       return False

    if None != args.logfile:
       Log.setLogfile(args.logfile)

    if False == args.verbose:
        Log.setLevel(logging.ERROR)

    else:
        Log.setLevel(logging.INFO)


    Log.getLogger().info("")

    ReadConfigFile(args.input,args.output)


if __name__ == '__main__':
        main()
