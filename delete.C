#include "catalog.h"
#include "page.h"
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

const Status QU_Delete(const string &relation,
					   const string &attrName,
					   const Operator op,
					   const Datatype type,
					   const char *attrValue)
{
	// part 6
	cout << "Doing QU_Delete " << endl;
	Status status;
	AttrDesc delAttr;
	// get attribute descriptor for deletion attribute
	const char *filterPtr = attrValue; // default for STRING or no predicate
	if (!attrName.empty())
	{ // special case: if attrName is empty, delete all records
		status = attrCat->getInfo(relation, attrName, delAttr);
		if (status != OK)
		{
			return status;
		}
		// check that type matches
		if (delAttr.attrType != type)
		{
			return ATTRTYPEMISMATCH;
		}
		
		int filterInt = 0;
		float filterFloat = 0.0f;
		switch (type)
		{	
		case INTEGER:
		
			filterInt = atoi(attrValue);
			filterPtr = reinterpret_cast<const char *>(&filterInt);
			break;
		case FLOAT:
		
			filterFloat = (float)atof(attrValue);
			filterPtr = reinterpret_cast<const char *>(&filterFloat);
			break;
		case STRING:
			// leave as-is
			filterPtr = attrValue;
			break;
		}
	}

	// set up heap file scan
	HeapFileScan *hfs;
	hfs = new HeapFileScan(relation, status);
	if (status != OK)
	{
		delete hfs;
		return status;
	}

	// start scan
	status = hfs->startScan(delAttr.attrOffset, delAttr.attrLen, (Datatype)delAttr.attrType,
							(attrValue != nullptr) ? filterPtr : NULL, op);

	if (status != OK)
	{
		delete hfs;
		return status;
	}
	RID rid;
	Record rec;
	// scan through records and delete them
	while ((status = hfs->scanNext(rid)) == OK)
	{
		status = hfs->deleteRecord();
		if (status != OK)
		{
			hfs->endScan();
			delete hfs;
			return status;
		}
	}
	Status nextStatus = hfs->endScan();
	delete hfs;
	if (nextStatus != OK && nextStatus != FILEEOF)
	{ // Either deletes or does not find the record (EOF is ok)
		return nextStatus;
	}

	return OK;
}
