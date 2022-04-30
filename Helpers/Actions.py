from os.path import exists
import pickle
import copy
import fnmatch

from Helpers import Log
from Data import MarvinGroupData

def Matches(name,pattern):
    return fnmatch.fnmatch(name.upper(),pattern.upper())

def HandleWildcardUpdate(inpString,pattern):
    return pattern.replace('*',inpString)


def mergeLists(srcList,listToMerge):
    if srcList == None:
        return list(listToMerge)

    if len(srcList) == 0:
        return list(listToMerge)

    srcList.extend(listToMerge)

    mergedList = sorted(srcList,key=lambda entry: entry.ArrivalTime)

    return mergedList

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

    #entry.Value = "{:.2f}".format(float(entry.Value))
    #entry.Value = float(entry.Value)

    if None != min:
        try:
            min = float(min)
            if float(entry.Value) < min:
                #entry.Value = "{:.2f}".format(min)
                entry.Value = str(min)
                retVal = True
        except:
            Log.getLogger().error("Invalid <Namespace> - Min BoundID value of " + min +" is invalid.")
            raise pickle.UnpicklingError()

    if None != max:
        try:
            max = float(max)
            if float(entry.Value) > max:
                #entry.Value = "{:.2f}".format(max)
                entry.Value = str(max)
                retVal = True
        except:
            Log.getLogger().error("Invalid <Namespace> - Max BoundID value of " + max +" is invalid.")
            raise pickle.UnpicklingError()

    return retVal

def DeltaValue(entry,delta):
    retVal = False
    try:
        float(entry.Value)
    except:
        Log.getLogger().info("Invalid <Namespace> - BoundID tried to bound non numeric data point, ID="+entry.ID)
        return


    if None != delta:
        try:
            delta = float(delta)
            entry.Value = str(float(entry.Value) + delta)
            retVal = True
        except:
            Log.getLogger().error("Invalid <Namespace> - delta  value of " + delta +" is invalid.")
            raise pickle.UnpicklingError()

    return retVal    


class FileHandler(object):
    def __init__(self,inpFname):
        self._sourceFile = inpFname
        Log.getLogger().info("Processing " + self._sourceFile)
        with open(self._sourceFile,'rb') as fp:
            try:
                self._entries = pickle.load(fp)
                
            except pickle.UnpicklingError as ex:
                Log.getLogger().error("Invlid BIFF save file specified: " + self._sourceFile)
                raise

        entryCount = self.createNamespaceMap()

#        Log.getLogger().info(self._sourceFile + " contains " + str(len(self._namespaceMap)) + " namespaces and " + str(entryCount) + " datapoints.")

    def getMatchingNamespacesNameList(self,pattern):
        retList=[]
        for namespaceName in self._namespaceMap:
            if Matches(namespaceName,pattern):
                retList.append(namespaceName)
        
        return retList


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

            self._namespaceMap[namespace].append(entry)
            
        return entryCount

    # retuns the final list of all the namespaces for this file after all the manipulations
    def createMergedList(self,offsetTime=0):
        resultList=None
        for namespace in self._namespaceMap:
                resultList = mergeLists(resultList,self._namespaceMap[namespace])

        if offsetTime > 0:
            for entry in resultList:
                entry.ArrivalTime += offsetTime

        return resultList


    def writeFile(self,outfile,overWrite):
        if False == overWrite and exists(outfile):
            overwrite = input('{} already exists. Overwrite? Y = yes, N = no\n'.format(outfile))
            if overwrite.lower() == 'y':                
                pass
            else:
                Log.getLogger().warn("Skipping writing to file {}".format(outfile))
                return

        resultList = self.createMergedList()
        try:
            with open(outfile,'w+b') as fp:
               pickle.dump(resultList, fp, pickle.DEFAULT_PROTOCOL)

            Log.getLogger().info("New file [" + outfile + "] created with " + str(len(resultList)) + " entries.")
        except Exception as ex:
            print(str(ex))
            return False

    # renames a namespace
    def Rename_Namespace(self,origName,newName):
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

    def Delete_Namespace(self,namespaceName):
        namespaces = self.getMatchingNamespacesNameList(namespaceName)
        if len(namespaces) > 0:
            for namespace in namespaces:
                del(self._namespaceMap[namespace])
                Log.getLogger().info("Namespace: {} has been deleted".format(namespace))

        else:
            Log.getLogger().error("Cannot delete namespace: {} - it does not exist".format(namespaceName))

        return len(namespaces)

    def Delete_Id(self,namespaceName,ids):
        namespaces = self.getMatchingNamespacesNameList(namespaceName)
        
        totalRemovedCount = 0
        if not isinstance(ids,list):
            ids = [ids]

        if len(namespaces) > 0:
            for namespace in namespaces:
                newList = []
                removedCount = 0

                for entry in self._namespaceMap[namespace]:
                    for id in ids:
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

                totalRemovedCount += removedCount

        else:
            Log.getLogger().error("Namespace: {} does not exist".format(namespaceName))

        return totalRemovedCount

    def Rename_Namespace(self,origName,newName):
        namespaces = self.getMatchingNamespacesNameList(origName)

        for namespace in namespaces:
            newNamespaceName = HandleWildcardUpdate(namespace,newName)
            for entry in self._namespaceMap[namespace]:  # go through each entry and update namespace
                if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                    for subEntry in entry._DataList:
                        subEntry.Namespace = newNamespaceName
                else:
                    entry.Namespace = newNamespaceName

        return len(namespaces)


    def Rename_Id(self,namespaces,ids,newName):
        changedCount = 0
        idFoundMap={}
        if not isinstance(ids,list):
            ids = [ids]
        namespaces = self.getMatchingNamespacesNameList(namespaces)
        for namespace in namespaces:
            for searchId in ids:
                for entry in self._namespaceMap[namespace]:
                    if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                        for subEntry in entry._DataList:
                            if Matches(subEntry.ID,searchId):
                                NewID =  HandleWildcardUpdate(subEntry.ID,newName)
                                subEntry.ID = NewID
                                changedCount += 1
                                NewID= NewID.lower()
                                if NewID not in idFoundMap:
                                    idFoundMap[NewID] = NewID


                    elif Matches(entry.ID.lower(),searchId):
                        NewID =  HandleWildcardUpdate(entry.ID,newName)
                        entry.ID = NewID
                        changedCount += 1
                        NewID= NewID.lower()
                        if NewID not in idFoundMap:
                            idFoundMap[NewID] = NewID

        return (changedCount,len(idFoundMap))

    def Copy_Namespace(self,origName,newName):
        namespaces = self.getMatchingNamespacesNameList(origName)
        copiedCount=0

        for namespace in namespaces:
            newNamespaceName = HandleWildcardUpdate(namespace,newName)
            if newNamespaceName in self._namespaceMap[namespace]:
                Log.getLogger().error("Cannot copy namespace {} to {} - it already exists".format(namespace,newNamespaceName))

            else:
                copiedCount+=1
                self._namespaceMap[newNamespaceName] = copy.deepcopy(self._namespaceMap[namespace])
                for entry in self._namespaceMap[newNamespaceName]:
                    if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                        for subEntry in entry._DataList:
                            subEntry.Namespace = newNamespaceName
                    else:
                        entry.Namespace = newNamespaceName

        return copiedCount

    def Copy_Id(self,namespaces,ids,newNs,newId):
        changedCount = 0
        
        if not isinstance(ids,list):
            ids = [ids]
        namespaces = self.getMatchingNamespacesNameList(namespaces)
        for namespace in namespaces:
            temporaryNS=[]
            NewNS =  HandleWildcardUpdate(namespace,newNs)
            for searchId in ids:
                for entry in self._namespaceMap[namespace]:
                    if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                        assert(False,"Copy id for MarvinDataGrou not supported yet")

                    elif Matches(entry.ID.lower(),searchId):
                        NewID =  HandleWildcardUpdate(entry.ID,newId)
                        newEntry = copy.deepcopy(entry)
                        newEntry.ID = NewID
                        newEntry.Namespace = NewNS
                        temporaryNS.append(newEntry)
                        changedCount+=1

            if NewNS in self._namespaceMap:
                mergeLists(self._namespaceMap[NewNS],temporaryNS)

            else:
                self._namespaceMap[NewNS] = temporaryNS
                
        return changedCount


    def Bound_Id(self,namespaceName,ids,maxValue,minValue):
        namespaces = self.getMatchingNamespacesNameList(namespaceName)
        
        totalModifiedCount = 0
        if not isinstance(ids,list):
            ids = [ids]

        if len(namespaces) > 0:
            for namespace in namespaces:
                for searchId in ids:
                    for entry in self._namespaceMap[namespace]:
                        if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                            for subEntry in entry._DataList:
                                if Matches(subEntry.ID,searchId):
                                    if BoundValue(subEntry,minValue,maxValue):
                                       totalModifiedCount += 1 

                        elif Matches(entry.ID.lower(),searchId):
                            if BoundValue(entry,minValue,maxValue):
                                totalModifiedCount += 1 

        else:
            Log.getLogger().error("Namespace: {} does not exist".format(namespaceName))

        return totalModifiedCount

    def ApplyDelta_Id(self,namespaceName,ids,deltaVal):
        namespaces = self.getMatchingNamespacesNameList(namespaceName)
        
        totalModifiedCount = 0
        if not isinstance(ids,list):
            ids = [ids]

        if len(namespaces) > 0:
            for namespace in namespaces:
                for searchId in ids:
                    for entry in self._namespaceMap[namespace]:
                        if isinstance(entry,MarvinGroupData.MarvinDataGroup):
                            for subEntry in entry._DataList:
                                if Matches(subEntry.ID,searchId):
                                    if DeltaValue(subEntry,deltaVal):
                                       totalModifiedCount += 1 

                        elif Matches(entry.ID.lower(),searchId):
                            if DeltaValue(entry,deltaVal):
                                totalModifiedCount += 1 

        else:
            Log.getLogger().error("Namespace: {} does not exist".format(namespaceName))

        return totalModifiedCount  