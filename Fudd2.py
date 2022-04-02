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
import pathlib
from pprint import pprint as pprint
import glob
from xml.parsers.expat import ExpatError

from Helpers import Log
from Helpers import FileHandler
from Helpers import Actions
from Helpers import VersionMgr

g_args=None

def existFile(filename):
    if not os.path.exists(filename):
        Log.getLogger().error("Specified file: " + str(filename) + " does not exist.")
        return False
    return True


def GetTargetFileName(inpName,destInfo):
    tDir= os.path.dirname(destInfo)
    tName = pathlib.Path(destInfo).stem
    tExt = pathlib.Path(destInfo).suffix
    if None != tExt and '.' == tExt[0] and tExt.count('.')>1:
        tExt = tExt[1:]

    inpFname = pathlib.Path(inpName).stem
    inpExt = pathlib.Path(inpName).suffix
    newFileName = tDir + os.sep
    newFileName += tName.replace('*',inpFname)
    newFileName += tExt.replace('*',inpExt)

    return newFileName

def test(inpList,destInfo):
    for inpName in inpList:
        newFileName = GetTargetFileName(inpName,destInfo)
        print(newFileName)


def main():
    if not HandleCommandlineArguments():
        return

class ArgObject(object):
    def __init__(self):
        self._parser=None
        self._workerFn=None
        self._action=None
        self._desc=None

    def getDescription(self):
        return self._desc

    def getAction(self):
        return self._action

    def getActionList(self):
        if isinstance(self._parser,argparse.ArgumentParser):
            return []

        return self._parser

    def getParser(self):
        if isinstance(self._parser,argparse.ArgumentParser):
            return self._parser

        return None

    def getWorkerFn(self):
        return self._workerFn

    def getActionStrings(self):
        if isinstance(self._parser,argparse.ArgumentParser):
            return [self._action]

        retList=[]
        for obj in self._parser: #is a list of more things
            retList.append(obj.getAction())

        return retList

def deleteNamespace(args):
    inpFiles =  glob.glob(g_args.input)
    totalDeleted=0
    fCount=0

    for inpName in inpFiles:
        fHandler = Actions.FileHandler(inpName)
        deletedFromFileCount = 0
        for namespace in args.namespace:
            deletedFromFileCount += fHandler.Delete_Namespace(namespace)

        targetFn = GetTargetFileName(inpName,g_args.output)
        
        fHandler.writeFile(targetFn,g_args.overwrite)
        Log.getLogger().info("{} namespaces deleted from {}".format(deletedFromFileCount,inpName))
        totalDeleted+=deletedFromFileCount
        if deletedFromFileCount > 0:
            fCount +=1

    Log.getLogger().info("Deleted {} namespaces deleted from {} files".format(totalDeleted,fCount))


def deleteId(args):
    inpFiles =  glob.glob(g_args.input)
    totalDeleted=0
    fCount=0

    for inpName in inpFiles:
        fHandler = Actions.FileHandler(inpName)
        deletedFromFileCount = 0
        for namespace in args.namespace:
            deletedFromFileCount += fHandler.Delete_Id(namespace,args.id)

        targetFn = GetTargetFileName(inpName,g_args.output)
        fHandler.writeFile(targetFn,g_args.overwrite)
        Log.getLogger().info("{} datapoints deleted from {}".format(deletedFromFileCount,inpName))
        totalDeleted+=deletedFromFileCount
        if deletedFromFileCount > 0:
            fCount +=1

    Log.getLogger().info("Deleted {} datapoints  from {} files".format(totalDeleted,fCount))

def renameNamespace(args):
    inpFiles =  glob.glob(g_args.input)
    totalRenamed=0
    fCount=0

    for inpName in inpFiles:
        fHandler = Actions.FileHandler(inpName)
        renamedInFileCount = 0
        for namespace in args.namespace:
            renamedInFileCount += fHandler.Rename_Namespace(namespace,args.new)

        targetFn = GetTargetFileName(inpName,g_args.output)
        fHandler.writeFile(targetFn,g_args.overwrite)
        Log.getLogger().info("{} namespaces renamed in {}".format(renamedInFileCount,inpName))
        totalRenamed+=renamedInFileCount
        if renamedInFileCount > 0:
            fCount +=1

    Log.getLogger().info("Renamed {} namespaces int {} files".format(totalRenamed,fCount))

def renameId(args):
    inpFiles =  glob.glob(g_args.input)
    totalRenamedPoints=0
    totalIdsChanged=0
    fCount=0

    for inpName in inpFiles:
        fHandler = Actions.FileHandler(inpName)
        pointInFileChanged=0
        idsInFileChanged=0
        for namespace in args.namespace:
            pointCount, idCount = fHandler.Rename_Id(namespace,args.id,args.new)
            pointInFileChanged += pointCount
            idsInFileChanged += idCount

        targetFn = GetTargetFileName(inpName,g_args.output)
        fHandler.writeFile(targetFn,g_args.overwrite)
        Log.getLogger().info("{} IDs, {} dataponts renamed in {}".format(idsInFileChanged,pointInFileChanged,inpName))
        totalRenamedPoints+=pointInFileChanged
        totalIdsChanged+=idsInFileChanged
        if idsInFileChanged > 0:
            fCount +=1

    Log.getLogger().info("Renamed {} ids in {} files, {} datapoints".format(totalIdsChanged,fCount,totalRenamedPoints))


def copyNamespace(args):
    inpFiles =  glob.glob(g_args.input)
    totalCopied=0
    fCount=0

    for inpName in inpFiles:
        fHandler = Actions.FileHandler(inpName)
        copiedInFileCount = 0
        for namespace in args.namespace:
            copiedInFileCount += fHandler.Copy_Namespace(namespace,args.new)

        targetFn = GetTargetFileName(inpName,g_args.output)
        fHandler.writeFile(targetFn,g_args.overwrite)
        Log.getLogger().info("{} namespaces copied in {}".format(copiedInFileCount,inpName))
        totalCopied+=copiedInFileCount
        if copiedInFileCount > 0:
            fCount +=1

    Log.getLogger().info("Copied {} namespaces in {} files".format(totalCopied,fCount))

def copyId(args):
    inpFiles =  glob.glob(g_args.input)
    totalCopiedPoints=0
    
    fCount=0

    for inpName in inpFiles:
        fHandler = Actions.FileHandler(inpName)
       
        for namespace in args.namespace:
            pointsCopied = fHandler.Copy_Id(namespace,args.id,args.newNamespace,args.newId)

        targetFn = GetTargetFileName(inpName,g_args.output)
        fHandler.writeFile(targetFn,g_args.overwrite)
        Log.getLogger().info("{} dataponts copied in {}".format(pointsCopied,inpName))
        totalCopiedPoints+=pointsCopied

        if pointsCopied > 0:
            fCount +=1

    Log.getLogger().info("Copied {} datapoints in {} files".format(totalCopiedPoints,fCount))


def AddActionParserToList(dataList,actionString,parser,fn=None):
    newObj = ArgObject()
    newObj._action = actionString
    newObj._parser = parser
    newObj._workerFn = fn

    dataList.append(newObj)

def SetupActions():

    foo = ArgObject()
    foo._parser = []
    actionList = foo._parser

    actionDeleteList=[]
    parser = argparse.ArgumentParser(description='delete namespace',add_help=True, usage='''delete namespace -n namesspace
    More than one namespace can be specified''')
    parser.add_argument("-n","--namespace",help="namespace to delete", nargs="+", type=str,required=True)

    AddActionParserToList(actionDeleteList,"namespace",parser,deleteNamespace)

    parser = argparse.ArgumentParser(description='delete id',add_help=True,usage='''delete id -n namesspace -id id
      wildcard allowed for namespace and id''')
    parser.add_argument("-n","--namespace",type=str,required=True,nargs="+")
    parser.add_argument("--id",type=str,required=True,nargs="+")
    AddActionParserToList(actionDeleteList,"id",parser,deleteId)

    AddActionParserToList(actionList,"delete",actionDeleteList)

    #### rename ####
    actionRenameList=[]
    parser = argparse.ArgumentParser(description='rename namespace',add_help=True,usage='''rename namespace -n namesspace --new ns
    More than one namespace can be specified and wildcards''')
    parser.add_argument("-n","--namespace",type=str,required=True,nargs="+")
    parser.add_argument("-new",type=str,required=True)
    AddActionParserToList(actionRenameList,"namespace",parser,renameNamespace)

    parser = argparse.ArgumentParser(description='rename id',add_help=True,usage='''rename id -n namesspace -id --new id
    More than one namespace can be specified and wildcards are valid for both hamespace and id''')
    parser.add_argument("-n","--namespace",type=str,required=True,nargs="+")
    parser.add_argument("--id",type=str,required=True,nargs="+")
    parser.add_argument("-new",type=str,required=True)
    AddActionParserToList(actionRenameList,"id",parser,renameId)

    AddActionParserToList(actionList,"rename",actionRenameList)

    #### copy ####
    actionCopyList=[]
    parser = argparse.ArgumentParser(description='copy specified namespace to another namespace',add_help=True,usage='''copy namespace -n namesspace --new ns
    More than one namespace can be specified and wildcards''')
    parser.add_argument("-n","--namespace",type=str,required=True,nargs="+")
    parser.add_argument("-new",type=str,required=True)
    AddActionParserToList(actionCopyList,"namespace",parser,copyNamespace)

    parser = argparse.ArgumentParser(description='copy specified id to another id',add_help=True,usage='''copy id -n namespace -id id --newNamespace namespace --newId id
    More than one namespace can be specified and wildcards are valid for both hamespace and id''')
    parser.add_argument("-n","--namespace",type=str,required=True,nargs="+")
    parser.add_argument("--id",type=str,required=True,nargs="+")
    parser.add_argument("--newNamespace",type=str,default="*")
    parser.add_argument("--newId",type=str,required=True)
    AddActionParserToList(actionCopyList,"id",parser,copyId)

    AddActionParserToList(actionList,"copy",actionCopyList)


    return foo.getActionStrings(), foo

def parseAndRunAction(actionName,actionList,argList,usageList=""):
    if isinstance(actionList,ArgObject): # is an argparse object
        argObj = actionList
        parser = argObj.getParser()
        args = argObj.getParser().parse_args(argList)
        try:
            argObj.getWorkerFn()(args)
        except Exception as Ex:
            Log.getLogger().error(str(Ex))
        return

    for currAction in actionList:
        if currAction.getAction() == actionName:
            if None != currAction.getParser():
                args = currAction.getParser().parse_args(argList)
                break
            else: #is a list of args
                validOptions=currAction.getActionStrings()
                requestedAction = [argList[0]]
                otherArgs=argList[1:]
                parser = argparse.ArgumentParser()
                parser.add_argument(actionName, help=actionName+' to perform', 
                        nargs="?",
                        choices=validOptions,
                        )                
                parser.parse_args(requestedAction)
                for actionItem in currAction.getActionList():
                    if actionItem.getAction() == requestedAction[0]: # requestedAction is in array for argPArse
                        return parseAndRunAction(requestedAction,actionItem,otherArgs)
    

def HandleCommandlineArguments():
    print("FUDD - BIFF Save File Editor Version " + VersionMgr.ReadVer())

    if sys.version_info < (3, 3):
        print("---- Error: Required Python 3.3 or greater ---")
        return False
    
    firstLevelActionNames,firstLevelActions=SetupActions()
    parser = argparse.ArgumentParser(description='FUDD the Elmer',add_help=False)

    parser.add_argument("-i","--input",help='specifies input file(s), wildcards allowed',type=str,required=True)
    parser.add_argument("-o","--output",help='specifies file to generate, wildcards allowed in most cases',type=str,required=True)
    parser.add_argument('-a','--action', help='action to perform', required=True,
                        nargs="?",
                        choices=firstLevelActionNames,
                        )


    parser.add_argument("-y","--overwrite",help="will not prompt if overwriting target",action="store_true")
    parser.add_argument("-l","--logfile",help='specifies log file name',type=str)
    parser.add_argument("-v","--verbose",help="prints debug information",action="store_true")
    parser.add_argument('-h','--help', action='store_true')

    try:    
        global g_args
        g_args,sub_args = parser.parse_known_args()

        # Manually handle help
        if len(sub_args) < 1:
                print(parser.format_help())
                sys.exit(1)
            # Otherwise pass the help option on to the subcommand
        if g_args.help:
            sub_args.append('--help')


    except:
       return False

    if None != g_args.logfile:
       Log.setLogfile(g_args.logfile)

    if False == g_args.verbose:
        Log.setLevel(logging.ERROR)

    else:
        Log.setLevel(logging.INFO)

    parseAndRunAction(g_args.action,firstLevelActions.getActionList(),sub_args)
    

if __name__ == '__main__':
        main()
