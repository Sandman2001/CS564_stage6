#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 * 	A selection is implemented using a filtered HeapFileScan.  
 * 	The result of the selection is stored in the result relation called result (a  heapfile with this name will be created by the parser before QU_Select() is called).  
 * 	The project list is defined by the parameters projCnt and projNames.  
 *  Projection should be done on the fly as each result tuple is being appended to the result table.  
 * 	A final note: the search value is always supplied as the character string attrValue.  
 *  You should convert it to the proper type based on the type of attr. 
 *  You can use the atoi() function to convert a char* to an integer and atof() to convert it to a float.   
 *  If attr is NULL, an unconditional scan of the input table should be performed.
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	//get attribute descriptors for projection attributes
	AttrDesc* projAttrs = new AttrDesc[projCnt];
	Status status;
	for (int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projAttrs[i]);
		if (status != OK) {
			delete[] projAttrs;
			return status;
		}
	}
	//get attribute descriptor for selection attribute, if any
	AttrDesc* selAttr = nullptr;
	if (attr != nullptr) {
		selAttr = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *selAttr);
		if (status != OK) {	
			delete[] projAttrs;
			delete selAttr;
			return status;
		}	
	}
	//call ScanSelect to do the actual work
	status = ScanSelect(result, projCnt, projAttrs, selAttr, op, attrValue,
		(selAttr != nullptr) ? selAttr->attrLen : 0);
	//clean up
	delete[] projAttrs;
	if (selAttr != nullptr)
		delete selAttr;
	return status;	
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	HeapFileScan* hfs;
	hfs = new HeapFileScan(attrDesc->relName, status);
	if (status != OK) {
		delete hfs;
		return status;
	}
	//start scan
	if (attrDesc != nullptr) {
		//convert filter value to proper type

		status = hfs->startScan(0, attrDesc->attrLen, (Datatype)attrDesc->attrType,
			filter, op);
		if (status != OK) {
			delete hfs;
			return status;
		}

	} else {
		//unconditional scan
		status = hfs->startScan();
		if (status != OK) {
			delete hfs;
			return status;
		}
	}
	//create result heap file
	HeapFile* resultHF = new HeapFile(result, status);
	if (status != OK) {
		hfs->endScan();
		delete hfs;
		return status;
	}

	RID rid;
	Record rec;
	//scan through records
	while ((status = hfs->scanNext(rid)) == OK) {
		//get record
		status = hfs->getRecord(rec);
		if (status != OK) {	
			hfs->endScan();
			delete hfs;
			delete resultHF;
			return status;
		}
		//project attributes
		char* data = new char[reclen];
		for (int i = 0; i < projCnt; i++) {
			//get attribute value from record
			void* attrData = (char*)rec.data + projNames[i].attrOffset;
			//copy to data area at correct offset
			memcpy(data + projNames[i].attrOffset, attrData, projNames[i].attrLen);
		}
		//create new record
		Record outRec;
		outRec.data = data;
		outRec.length = reclen;
		//insert record into result heap file
		RID outRid;
		status = resultHF->insertRecord(outRec, outRid);
		//clean up
		delete[] data;
		if (status != OK) {
			hfs->endScan();
			delete hfs;
			delete resultHF;
			return status;
		}
	}
	hfs->endScan();
	delete hfs;
	delete resultHF;
	return OK;
}
