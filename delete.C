#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 * This function will delete all tuples in relation satisfying the predicate specified by attrName, op, and the constant attrValue. 
 * type denotes the type of the attribute. You can locate all the qualifying tuples using a filtered HeapFileScan.
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6
	cout << "Doing QU_Delete " << endl;
	Status status;
	AttrDesc delAttr;
	//get attribute descriptor for deletion attribute
	status = attrCat->getInfo(relation, attrName, delAttr);
	if (status != OK) {
		return status;
	}
	//check that type matches
	if (delAttr.attrType != type) {
		return ATTRTYPEMISMATCH;
	}
	//set up heap file scan
	HeapFileScan* hfs;
	hfs = new HeapFileScan(relation, status);
	if (status != OK) {
		delete hfs;
		return status;
	}
	//convert filter value to proper type
	// void* value = nullptr;
	// switch (type) {
	// case INTEGER:
	// 	{
	// 		int* intVal = new int;
	// 		*intVal = atoi(attrValue);
	// 		value = intVal;
	// 		break;
	// 	}
	// case FLOAT:
	// 	{
	// 		float* floatVal = new float;
	// 		*floatVal = atof(attrValue);
	// 		value = floatVal;
	// 		break;
	// 	}
	// case STRING:
	// 	{
	// 		value = (void*)attrValue;
	// 		break;
	// 	}
	// }
	// //start scan
	status = hfs->startScan(0, delAttr.attrLen, (Datatype)delAttr.attrType,
		attrValue, op);
	//clean up
	// if (type == INTEGER)
	// 	delete (int*)value;
	// else if (type == FLOAT)
	// 	delete (float*)value;
	if (status != OK) {	
		delete hfs;
		return status;
	}	
	RID rid;
	//scan through records and delete them
	while ((status = hfs->scanNext(rid)) == OK) {
		status = hfs->deleteRecord();
		if (status != OK) {	
			hfs->endScan();
			delete hfs;
			return status;
		}
	}
	Status nextStatus = hfs->endScan();
	if (status == OK) status = nextStatus;
	delete hfs;
	if (status == FILEEOF)
return OK;



}


