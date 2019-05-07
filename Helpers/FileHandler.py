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
#   Where the processing of the namespaces, files etc occurs
#
##############################################################################
import os
import logging
import xml.dom.minidom
import pickle
import copy
import fnmatch
from  pprint import pprint

from Helpers import Log
from Data import MarvinGroupData
from Data import MarvinData


def Matches(name,pattern):
    return fnmatch.fnmatch(name.upper(),pattern.upper())

# Just a helper in parsing XML, sends just the child nodes that match
def getChildNodes(baseNode,childName):
    retList=[]
    for child in baseNode.childNodes:
        if child.nodeName == childName:  # could make this case independent if wanted to
            retList.append(child)

    return retList

# Will scale a given value - must be numeric
def ScaleValue(entry,scaleVal,Precision):
    try:
        fVal = float(entry.Value)
        scaleVal = float(scaleVal)
    except:
        Log.getLogger().info("Failed to scale ID: "  + entry.ID + " as it is not a numeric value.  Ignoring.")
        return

    fVal *= scaleVal
    if None != Precision:
        fVal = format( fVal,'.' + str(Precision) + 'f')

    entry.Value = str(fVal)
        

# if the value is outside of the min/max passed, set it to the min/max value
def BoundValue(entry,min,max):
    retVal = False
    if None == min and None == max:
        Log.getLogger().error("Invalid <Namespace> - BoundID without Min or Max value specified.")
        raise pickle.UnpicklingError()

    try:
        float(entry.Value)
    except:
        Log.getLogger().info("Invalid <Namespace> - BoundID tried to bound non numeric data point, ID="+entry.ID)
        return


    if None != min:
        try:
            min = float(min)
            if float(entry.Value) < min:
                entry.Value = str(min)
                retVal = True
        except:
            Log.getLogger().error("Invalid <Namespace> - Min BoundID value of " + min +" is invalid.")
            raise pickle.UnpicklingError()

    if None != max:
        try:
            max = float(max)
            if float(entry.Value) > max:
                entry.Value = str(max)
                retVal = True
        except:
            Log.getLogger().error("Invalid <Namespace> - Max BoundID value of " + max +" is invalid.")
            raise pickle.UnpicklingError()

    return retVal


## helper routine, combines list 1 and list 2, sorted by Arrival Time
def mergeLists(srcList,listToMerge):
    if srcList == None:
        return list(listToMerge)

    if len(srcList) == 0:
        return list(listToMerge)

    srcList.extend(listToMerge)

    mergedList = sorted(srcList,key=lambda entry: entry.ArrivalTime)

    return mergedList

## my worker class that does all the real work
class FileHandler(object):
    def __init__(self,baseNode):
        self._sourceFile = baseNode.attributes["File"].nodeValue
        Log.getLogger().info("Processing " + self._sourceFile)
        with open(self._sourceFile,'rb') as fp:
            try:
                self._entries = pickle.load(fp)
                
            except pickle.UnpicklingError as ex:
                Log.getLogger().error("Invlid BIFF save file specified: " + self._sourceFile)
                raise

        self.getInsertTime(baseNode)
        entryCount = self.createNamespaceMap()
        Log.getLogger().info(self._sourceFile + " contains " + str(len(self._namespaceMap)) + " namespaces and " + str(entryCount) + " datapoints.")

        self.HandleIndividualNamespaceProcessing(baseNode)
        self.ProcessFileActions(baseNode)

    # reads time to insert from config time, could be 'Append' or an actual value, or doesn't exist
    def getInsertTime(self,baseNode):
        insertTimeEntry = getChildNodes(baseNode,"InsertTime")
        
        if None == insertTimeEntry or 0 == len(insertTimeEntry):
            self.insertTime = None

        elif len(insertTimeEntry) > 1:
            Log.getLogger().error("Only 1 insert time per source.")
            raise pickle.UnpicklingError()

        elif insertTimeEntry[0].firstChild.nodeValue == "Append":
            self.insertTime = "Append"
        else:
            try:
                self.insertTime = int(insertTimeEntry[0].firstChild.nodeValue)
            except:
                Log.getLogger().error("Invalid numeric value for <InsertTime>: " + insertTimeEntry[0].firstChild.nodeValue)
                raise pickle.UnpicklingError()

    ## nukes namespace from stream
    def ProcessRemoveNamespaces(self,baseNode):
        childNodes = getChildNodes(baseNode,"RemoveNamespace")
        for node in childNodes:
            namespace = node.firstChild.nodeValue
            if namespace in self._namespaceMap:
                del(self._namespaceMap[namespace])
            else:
                Log.getLogger().error("Invalid <RemoveNamespace> namespace: " + namespace + " does not exist")
                raise pickle.UnpicklingError()

    ## goes through the valid actions to perform on entire file, like when to insert, trim, etc.
    def ProcessFileActions(self,baseNode):
        for childNode in baseNode.childNodes:
            nodeName = childNode.nodeName
            if nodeName == "#text" or nodeName == '#comment':
                continue

            action = nodeName.lower()

            if action == "inserttime":
                pass
            elif action == "trim":
                ## Handle <Trim>
                trimEntry = getChildNodes(baseNode,"Trim")
        
                if None == trimEntry or 0 == len(trimEntry):
                    trimStart = None #indicate natural insertion time
                    trimEnd = None #indicate natural insertion time

                elif len(trimEntry) > 1:
                    Log.getLogger().error("Only 1 <Trim> per source.")
                    raise pickle.UnpicklingError()
                else:
                    try:
                        trimStart = int(getChildNodes(trimEntry[0],"StartTime")[0].firstChild.nodeValue)
                        trimEnd = int(getChildNodes(trimEntry[0],"EndTime")[0].firstChild.nodeValue)
                    except Exception as Ex:
                        Log.getLogger().error("Invalid <Trim>.  Must have valid <StartTime> and <EndTime>.")
                        raise pickle.UnpicklingError()

                    for namespace in self._namespaceMap:
                        self.TrimNamespace(namespace,trimStart,trimEnd)
            
            elif action == "span":
                ## Handle <Span>
                spanEntry = getChildNodes(baseNode,"Span")
        
                if None == spanEntry or 0 == len(spanEntry):
                    spanTime = None #indicate natural insertion time

                elif len(spanEntry) > 1:
                    Log.getLogger().error("Only 1 <Span> per source.")
                    raise pickle.UnpicklingError()

                else:
                    try:
                        runTime = getChildNodes(spanEntry[0],"RunTime")
                        if len(runTime) == 0:
                            Log.getLogger().error("Invalid <Span>, must have <Runtime>: ")
                            raise pickle.UnpicklingError()

                        if len(runTime) > 1:
                            Log.getLogger().error("Invalid <Span>, only 1 <Runtime>")
                            raise pickle.UnpicklingError()

                        spanTime = int(runTime[0].firstChild.nodeValue)
                    except Exception as Ex:
                        Log.getLogger().error("Invalid numeric value for <Span>: " + runTime[0].firstChild.nodeValue)
                        raise pickle.UnpicklingError()

                    for namespace in self._namespaceMap:
                        self.SpanNamespaceWorker(namespace,spanTime)

            elif action == "removenamespace":
                pass
            elif action == "namespace":
                pass
            else:
                Log.getLogger().error("Invalid <Source> option <" + nodeName + ">")
                raise pickle.UnpicklingError()
        self.ProcessRemoveNamespaces(baseNode)
                    

    def HandleIndividualNamespaceProcessing(self,baseNode):
        ## Handle <Namespace>
        namespaces = getChildNodes(baseNode,"Namespace")
        for namespace in namespaces:
            if "Name" in namespace.attributes:
                name = namespace.attributes["Name"].nodeValue
                self.ProcessNamespaceManipulation(namespace,name)

            else:
                Log.getLogger().error("Invalid <Namespace> - requires Name attribute.")
                raise pickle.UnpicklingError()

    # checks to see if an ID exists in a namespace
    def existsID(self,namespace,ID):
        ID = ID.lower()
        for entry in self._namespaceMap[namespace]:
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                for subEntry in entry._DataList:
                    if subEntry.ID.lower() == ID:
                        return True
            elif entry.ID.lower() == ID:
                return True

        return False

    # creates an array of data entries for every namespace in the file
    def createNamespaceMap(self):
        self._namespaceMap = {}
        entryCount = 0

        startTime = None

        for entry in self._entries:
            namespace = entry.Namespace
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                namespace=entry._DataList[0].Namespace
                entryCount += len(entry._DataList)

            else:
                entryCount += 1

            if not namespace in self._namespaceMap:
                self._namespaceMap[namespace] = []
                if None == startTime:
                    startTime = entry.ArrivalTime
                    if self.insertTime != "Append" and self.insertTime != None:
                        startTime -= self.insertTime


            entry.ArrivalTime -= startTime #reset arrival time based upon 1st packet
            self._namespaceMap[namespace].append(entry)
            
        return entryCount

    # renames a namespace
    def RenameNamespace(self,origName,newName):
        if newName in self._namespaceMap:
            Log.getLogger().error("<Namespace> rename failed - namespace " + newName + "already exists")
            raise pickle.UnpicklingError()

        for entry in self._namespaceMap[origName]:  # go through each entry and update namespace
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                for subEntry in entry._DataList:
                    subEntry.Namespace = newName
            else:
                entry.Namespace = newName


        #self._namespaceMap[newName] = self._namespaceMap.pop(origName)

        Log.getLogger().info("Renamed Namespace {} to {}".format(origName,newName))
        #self._namespaceMap[newName] = self._namespaceMap[origName]
        #del self._namespaceMap[origName]

    # makes a copy of a given namespace, with another name - is a copy not a rename
    def DuplicateNamespace(self,origName,newName):
        if newName in self._namespaceMap:
            Log.getLogger().error("<Namespace> duplicate failed - namespace " + newName + "already exists")
            raise pickle.UnpicklingError()

        self._namespaceMap[newName] = copy.deepcopy(self._namespaceMap[origName])
        for entry in self._namespaceMap[newName]:
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                for subEntry in entry._DataList:
                    subEntry.Namespace = newName
            else:
                entry.Namespace = newName


    # delete a datapoint from a namespace
    def DeleteDatapoint(self,namespace,baseNode):
        if not "ID" in baseNode.attributes:
            Log.getLogger().error("<Namespace> DeleteID failed - no ID attribute.")
            raise pickle.UnpicklingError()

        id = baseNode.attributes["ID"].nodeValue

        newList = []
        removedCount = 0

        for entry in self._namespaceMap[namespace]:
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                subList=[]
                for subEntry in entry._DataList:
                    if Matches(subEntry,id):
                        subList.append(subEntry)
                    else:
                        removedCount += 1

                if len(subList) != len(entry._DataList):
                    entry._DataList = subList

                newList.append(entry)

            else:
                if not Matches(entry.ID,id):
                    newList.append(entry)
                else:
                    removedCount += 1

        if removedCount > 0:
            self._namespaceMap[namespace] = newList

        else:
            Log.getLogger().error("<Namespace> Delete ID failed - no ID " + id + " not found.")
            #raise pickle.UnpicklingError()


        Log.getLogger().info("Removed " + str(removedCount) + " instances of ID: " + id)
        return removedCount

    # trims the namespace to a start and stop time
    def TrimNamespace(self,namespace,trimStart,trimEnd):
        if trimStart < 0:
            Log.getLogger().error("Invalid <Namespace> Trim - StartTime < 0.")
            raise pickle.UnpicklingError()

        if trimEnd < 0:
            Log.getLogger().error("Invalid <Namespace> Trim - EndTime < 0.")
            raise pickle.UnpicklingError()

        if trimEnd < trimStart:
            Log.getLogger().error("Invalid <Namespace> Trim - EndTime < StartTime.")
            raise pickle.UnpicklingError()

        newList=list(self._namespaceMap[namespace])
        l = len(newList)
        if len(newList) < 1:
            Log.getLogger().info("Asked to trim Namespace: " + namespace + ", however it is empty.  Skipping")
            return

        if self.insertTime == None or self.insertTime == 'Append':
            offset = 0 # need to account for 'insert time' 
        else:
            offset = self.insertTime

        index = 0
        if trimStart > 0:
            for entry in self._namespaceMap[namespace]:
                if entry.ArrivalTime - offset <= trimStart:
                    index +=1
                else:
                    break

        startIndex = index
        lastTime = newList[-1].ArrivalTime
        if trimEnd > newList[-1].ArrivalTime:
            Log.getLogger().info("<Namespace> Trim - EndTime > stream.  Ignoring.")

        else:
            index = 0
            for entry in newList:
                if entry.ArrivalTime - offset <= trimEnd:
                    index +=1
                else:
                    break

        endIndex = index
        try:
            self._namespaceMap[namespace] = self._namespaceMap[namespace][startIndex:endIndex]
        except:
            Log.getLogger().error("Can't trim Namespace: " + namespace + " to specified range, outside of bounds")


    # takes a 2nd namespace, renames it to the 1st namespace and returns one list with both contents
    def MergeNamespace(self,additionalNamespace,basenamespace):
        if not basenamespace in self._namespaceMap:
            Log.getLogger().error("<Namespace> MergeWith failed - unknown namespace:" + basenamespace)
            raise pickle.UnpicklingError()

        base = self._namespaceMap[basenamespace]
        del self._namespaceMap[basenamespace]
        self.DuplicateNamespace(additionalNamespace,basenamespace)
        other = self._namespaceMap[basenamespace]
    
        merged = mergeLists(base,other)
        
        self._namespaceMap[basenamespace] = merged

    # worker fucntion to Scale an ID within a namespace
    def ScaleID(self,namespace,node):
        if not "Factor" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - Scale - no Factor specified.")
            raise pickle.UnpicklingError()

        if not "ID" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - Scale - no ID specified.")
            raise pickle.UnpicklingError()

        idLow = node.attributes["ID"].nodeValue.lower()

        if not self.existsID(namespace,idLow):
            Log.getLogger().error("Invalid <Namespace> - Scale - ID: " + node.attributes["ID"].nodeValue + " does not exist.")
            raise pickle.UnpicklingError()

        if "Precision" in node.attributes:
            try:
                Precision = int(node.attributes["Precision"].nodeValue)
            except Exception as Ex:
                Log.getLogger().error("Invalid <Namespace> - Scale Precision - invalid value: " + node.attributes["Precision"])
                raise pickle.UnpicklingError()
        else:
            Precision = None
        try:
            factorVal = float(node.attributes["Factor"].nodeValue)
        except Exception as Ex:
            Log.getLogger().error("Invalid <Namespace> - Scale - invalid value: " + node.attributes["Factor"])
            raise pickle.UnpicklingError()

        scaleCount=0

        for entryObj in self._namespaceMap[namespace]:
            if isinstance(entryObj,MarvinGroupData.MarvinDataGroup):
                for entry in entryObj._DataList:
                    if entry.ID.lower() == idLow:
                        ScaleValue(entry,factorVal,Precision)
                        scaleCount += 1

            elif entryObj.ID.lower() == idLow:
                ScaleValue(entryObj,factorVal,Precision)
                scaleCount += 1

        return scaleCount

    # worker to bound and ID in a namespace
    def BoundID(self,namespace,node):
        if not "ID" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - Bound - no ID specified.")
            raise pickle.UnpicklingError()

        id = node.attributes["ID"].nodeValue

        # if not self.existsID(namespace,idLow):
        #     Log.getLogger().error("Invalid <Namespace> - Bound - ID: " + node.attributes["ID"].nodeValue + " does not exist.")
        #     raise pickle.UnpicklingError()
        
        max=None
        min=None

        if "Max" in node.attributes:
            max = node.attributes['Max'].nodeValue

        if "Min" in node.attributes:
            min = node.attributes['Min'].nodeValue

        boundCount=0

        for entryObj in self._namespaceMap[namespace]:
            if isinstance(entryObj,MarvinGroupData.MarvinDataGroup):
                for entry in entryObj._DataList:
                    if Matches(entryObj.ID,id):
                        if BoundValue(entry,min,max):
                            boundCount += 1

            elif Matches(entryObj.ID,id):
                oldVAl = entryObj.Value
                if BoundValue(entryObj,min,max):
                    boundCount += 1

        Log.getLogger().info("Bound {} entries".format(boundCount))
        return boundCount

    # worker to bound and ID in a namespace
    def AddValueToID(self,namespace,node):
        if not "ID" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - AddValue - no ID specified.")
            raise pickle.UnpicklingError()

        id = node.attributes["ID"].nodeValue
        idLow = node.attributes["ID"].nodeValue.lower()

        if not self.existsID(namespace,idLow):
            Log.getLogger().error("Invalid <Namespace> - AddValue - ID: " + node.attributes["ID"].nodeValue + " does not exist.")
            raise pickle.UnpicklingError()
        
        if "Value" in node.attributes:
            try:
                valueToAdd = float(node.attributes['Value'].nodeValue)
                parts = node.attributes['Value'].nodeValue.split(".")
                valuePrecision = 0
                if len(parts) > 1:
                  valuePrecision = len(parts[1])
                
            except Exception as Ex:
                Log.getLogger().error("Invalid <Namespace> - AddValue - ID: " + node.attributes["ID"].nodeValue + " has invalid Value: " + node.attributes['Value'].nodeValue)
                raise pickle.UnpicklingError()
        else:
           Log.getLogger().error("Invalid <Namespace> - AddValue - ID: " + node.attributes["ID"].nodeValue + " has no Value.")
           raise pickle.UnpicklingError()

        changedCount=0

        for entryObj in self._namespaceMap[namespace]:
            if isinstance(entryObj,MarvinGroupData.MarvinDataGroup):
                for entry in entryObj._DataList:
                    if entry.ID.lower() == idLow:
                        try:
                           fValue = float(entry.Value)
                           parts = entry.Value.split(".")
                           dataPtPrecision = 0
                           if len(parts) > 1:
                              dataPtPrecision = len(parts[1])
                              
                           if valuePrecision > dataPtPrecision:
                              dataPtPrecision = valuePrecision
                              
                           entry.Value = str(round(valueToAdd + fValue,dataPtPrecision))
                           changedCount += 1
                        except:
                           Log.getLogger().error("Invalid <Namespace> - AddValue - ID: " + node.attributes["ID"].nodeValue + " is not a numberic data point.")
                           raise pickle.UnpicklingError()


            elif entryObj.ID.lower() == idLow:
                try:
                    fValue = float(entryObj.Value)
                    parts = entryObj.Value.split(".")
                    dataPtPrecision = 0
                    if len(parts) > 1:
                        dataPtPrecision = len(parts[1])
                        
                    if valuePrecision > dataPtPrecision:
                     dataPtPrecision = valuePrecision
                    
                    entryObj.Value = str(round(valueToAdd + fValue,dataPtPrecision))
                    if len(entryObj.Value) > 6:
                        print(entryObj.Value)
                    changedCount += 1
                except Exception as Ex:
                    Log.getLogger().error("Invalid <Namespace> - AddValue - ID: " + node.attributes["ID"].nodeValue + " is not a numeric data point.")
                    raise pickle.UnpicklingError()

        Log.getLogger().info("Added Value of {0} to {1} instances of {2}".format(valueToAdd,changedCount,id))

        return changedCount

    def __InsertHelper(self,namespace,newEntry):
        Found = False
        for index,entry in enumerate(self._namespaceMap[namespace]):
            if newEntry.ArrivalTime <= entry.ArrivalTime:
                Found = True
                break

        if Found:
            self._namespaceMap[namespace].insert(index,newEntry)
            return index

        return None


    # insert a datapoint into a namesapce
    def InsertDatapoint(self,namespace,node):
        if not "ID" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - Insert - no ID specified.")
            raise pickle.UnpicklingError()

        if not "Value" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - Insert - no Value specified.")
            raise pickle.UnpicklingError()

        if not "Time" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - Insert - no Time specified.")
            raise pickle.UnpicklingError()

        if "Interval" in node.attributes:
            try:
                Interval = int(node.attributes["Interval"].nodeValue)
            except:
                Log.getLogger().error("Invalid <Namespace> - Insert - invalid Interval specified:" + node.attributes['Interval'].nodeValue)
                raise pickle.UnpicklingError()
        else:
            Interval = None

        try:
            insertTime = int(node.attributes['Time'].nodeValue)
        except:
            Log.getLogger().error("Invalid <Namespace> - Insert - invalid Time specified:" + node.attributes['Time'].nodeValue)
            raise pickle.UnpicklingError()

        if None == Interval:
            newObj = MarvinData.MarvinData(namespace,node.attributes['ID'].nodeValue,node.attributes['Value'].nodeValue,insertTime,'1.0',False)
            return self.__InsertHelper(namespace,newObj)
        else:
            retVal = True
            insertCount = 0
            while None != retVal:
                newObj = MarvinData.MarvinData(namespace,node.attributes['ID'].nodeValue,node.attributes['Value'].nodeValue,insertTime,'1.0',False)
                retVal = self.__InsertHelper(namespace,newObj)
                insertTime += Interval
                insertCount += 1

        return insertCount




    # finds all unique IDs in a namesapce, and then at beginning of the namespace inserts a defined value
    def InitializeAll(self,namespace,node):
        if not "Value" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - InitAll - no Value specified.")
            raise pickle.UnpicklingError()

        if not "Time" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - InitAll - no Time specified.")
            raise pickle.UnpicklingError()

        try:
            insertTime = int(node.attributes['Time'].nodeValue)
        except:
            Log.getLogger().error("Invalid <Namespace> - InitAll - invalid Time specified:" + node.attributes['Time'].nodeValue)
            raise pickle.UnpicklingError()

        uniqueMap={}
        Value = node.attributes['Value'].nodeValue

        index = 0
        for entry in self._namespaceMap[namespace]:
            if insertTime <= entry.ArrivalTime:
                break
            index += 1
        
        startList = list(self._namespaceMap[namespace]) # dup list for processing

        initCount = 0

        for entry in startList:
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                for subEntry in entry._DataList:
                    if not subEntry.ID in uniqueMap:
                        uniqueMap[subEntry.ID] = subEntry.ID # just keep track of them
                        newObj = MarvinData.MarvinData(namespace,subEntry.ID,Value,insertTime,'1.0',False)
                        self._namespaceMap[namespace].insert(index,newObj)
                        initCount += 1

            elif not entry.ID in uniqueMap:
                uniqueMap[entry.ID] = entry.ID # just keep track of them
                newObj = MarvinData.MarvinData(namespace,entry.ID,Value,insertTime,'1.0',False)
                self._namespaceMap[namespace].insert(index,newObj)
                initCount += 1

        return initCount
                
    # rename an ID within a namespace
    def RenameID(self,namespace,node):
        if not "ID" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - RenameID - no ID specified.")
            raise pickle.UnpicklingError()

        if not "NewID" in node.attributes:
            Log.getLogger().error("Invalid <Namespace> - RenameID - no NewID specified.")
            raise pickle.UnpicklingError()

        ID = node.attributes["ID"].nodeValue
        NewID = node.attributes["NewID"].nodeValue

        if not self.existsID(namespace,ID):
            Log.getLogger().error("Invalid <Namespace> - RenameID - ID: " + ID + " does not exist.")
            raise pickle.UnpicklingError()

        if self.existsID(namespace,NewID):
            Log.getLogger().error("Invalid <Namespace> - RenameID - ID: " + NewID + " already exists.")
            raise pickle.UnpicklingError()

        ID = ID.lower()

        changedCount = 0
        for entry in self._namespaceMap[namespace]:
            if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                for subEntry in entry._DataList:
                    if ID == subEntry.ID.lower():
                        subEntry.ID = NewID
                        changedCount += 1

            elif ID == entry.ID.lower():
                entry.ID = NewID
                changedCount += 1

        return changedCount

    # stretches or shrinks runtime for a namespace
    def SpanNamespaceWorker(self,namespace,runtime):
        firstTime = self._namespaceMap[namespace][0].ArrivalTime
        lastTime = self._namespaceMap[namespace][-1].ArrivalTime
        delta = lastTime - firstTime
        factor = runtime/delta

        for entry in self._namespaceMap[namespace]:
            entry.ArrivalTime = int(float(entry.ArrivalTime) * factor)

    # stretches or shrinks runtime for a namespace
    def SpanNamespace(self,namespace,node):
        children = getChildNodes(node,"RunTime")
        if len(children) == 0:
            Log.getLogger().error("Invalid <Namespace> - SpanNS - RunTime not specified.")
            raise pickle.UnpicklingError()
        
        if len(children) > 1:
            Log.getLogger().error("Invalid <Namespace> - SpanNS - only 1 RunTime allowed.")
            raise pickle.UnpicklingError()

        try:
            runTime = int(children[0].firstChild.nodeValue)
        except:
            Log.getLogger().error("Invalid <Namespace> - SpanNS RunTime value: " + children[0].firstChild.nodeValue)
            
        self.SpanNamespaceWorker(namespace,runTime)

        return runTime

    # gets all the differnt options specified for a given namespace internal manipulation and performs them  
    def ProcessNamespaceManipulation(self,baseNode,namespace):
        if not namespace in self._namespaceMap:
            matched=False
            for ns in self._namespaceMap:
                if Matches(ns,namespace):
                    self.ProcessNamespaceManipulation(baseNode,ns)
                    matched = True

            if not matched:
                Log.getLogger().error("Invalid <Namespace> - Name: " + namespace + " does not exist.")
                raise pickle.UnpicklingError()
            else:
                return

        Log.getLogger().info("Processing Namespace: " + namespace)
        first = True
        for childNode in baseNode.childNodes:
            nodeName = childNode.nodeName
            if nodeName == "#text" or nodeName == '#comment':
                continue
            
            if nodeName == "RenameNS":
                if True == first:
                    self.RenameNamespace(namespace,childNode.firstChild.nodeValue)
                    first = False

            elif nodeName == "DuplicateNS":
                self.DuplicateNamespace(namespace,childNode.firstChild.nodeValue)

            elif nodeName == "DeleteID":
                self.DeleteDatapoint(namespace,childNode)

            elif nodeName == "MergeWithNS":
                self.MergeNamespace(namespace,childNode.firstChild.nodeValue)
                
            elif nodeName == "TrimNS":
                trimEntry = getChildNodes(baseNode,"TrimNS")
        
                if len(trimEntry) > 1:
                    Log.getLogger().error("Only 1 <TrimNS> per Namespace.")
                    raise pickle.UnpicklingError()

                try:
                    trimStart = int(getChildNodes(trimEntry[0],"StartTime")[0].firstChild.nodeValue)
                    trimEnd = int(getChildNodes(trimEntry[0],"EndTime")[0].firstChild.nodeValue)
                except:
                    Log.getLogger().error("Invalid Namespace <TrimNS>.")
                    raise pickle.UnpicklingError()

                self.TrimNamespace(namespace,trimStart,trimEnd)

            elif nodeName == "ScaleID":
                self.ScaleID(namespace,childNode)
            
            elif nodeName == "BoundID":
                self.BoundID(namespace,childNode)

            elif nodeName == "AddValue":
                self.AddValueToID(namespace,childNode)

            elif nodeName == "InsertID":
                self.InsertDatapoint(namespace,childNode)
        
            elif nodeName == "InitAllID":
                self.InitializeAll(namespace,childNode)
                
            elif nodeName == "RenameID":
                self.RenameID(namespace,childNode)

            elif nodeName == "SpanNS":
                self.SpanNamespace(namespace,childNode)

            else:
                Log.getLogger().error("Invalid Namespace Option <" + nodeName +">.")
                raise pickle.UnpicklingError()

    # retuns the final list of all the namespaces for this file after all the manipulations
    def createMergedList(self,offsetTime=0):
        resultList=None
        for namespace in self._namespaceMap:
                resultList = mergeLists(resultList,self._namespaceMap[namespace])

        if offsetTime > 0:
            for entry in resultList:
                entry.ArrivalTime += offsetTime

        return resultList


