#include "catalog.h"
#include "error.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 * This will insert the given values as a tuple into the relation relname. 
 * Note that the values of the attributes may need to be reordered before the insertion is performed in 
 * order to conform to the offsets specified for each attribute in the AttrCat table.  
 * You can do this by using memcpy to move each attribute in turn to its proper offset in a temporary array before calling insertRecord.
 * 
 * Insert a tuple with the given attribute values (in attrList) in relation. 
 * The value of the attribute is supplied in the attrValue member of the attrInfo structure. 
 * Since the order of the attributes in attrList[] may not be the same as in the relation, you might have to rearrange them before insertion. 
 * If no value is specified for an attribute, you should reject the insertion as Minirel does not implement NULLs.
 *
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
	Status status;
  	RelDesc rd;
  	AttrDesc ad;
	int attrcnt_in_rel;
	//check that relation exists	
	status = relCat->getInfo(relation, rd);
	if (status != OK)
		return status;
	//check that attrCnt matches
	if (rd.attrCnt !=  attrCnt)
		return BADINSERTATTCNT;
	
	//get all attribute info and check types and lengths
	AttrDesc* allAttrs = nullptr;
	status = attrCat->getRelInfo(relation, attrcnt_in_rel, allAttrs); //The attrs array is allocated by this function, but it should be deallocated by the caller.
	if (status != OK)
		return status;
	//check that attrCnt matches
	if (attrcnt_in_rel != attrCnt) {
		free(allAttrs);
		return BADINSERTATTCNT;
	}
	//create record data area
	char* data = new char[PAGESIZE]; //assume tuple fits in one page
	memset(data, 0, PAGESIZE);
	//for each attribute in relation, find it in attrList and copy value to data area
	for (int i = 0; i < attrCnt; i++) {
		ad = allAttrs[i];
		//find attribute in attrList
		bool found = false;
		for (int j = 0; j < attrCnt; j++) {
			if (strcmp(ad.attrName, attrList[j].attrName) == 0)
			{
				//check type and length
				if (ad.attrType != attrList[j].attrType || ad.attrLen != attrList[j].attrLen) {
					free(allAttrs);
					delete[] data;
					return BADINSERTPARM;
				}
				//copy value to data area at correct offset
				memcpy(data + ad.attrOffset, attrList[j].attrValue, ad.attrLen);
				found = true;
				break;
			}
		}
		if (!found) {
			//attribute not found in attrList
			free(allAttrs);
			delete[] data;
			return BADINSERTPARM;
		}
	}
	//create record
	Record rec;
	rec.data = data;
	rec.length = 0;
	//calculate total length of record
	for (int i = 0; i < attrCnt; i++) {
		rec.length += allAttrs[i].attrLen;
	}
	//insert record into heap file
	HeapFile hf(relation, status);
	if (status != OK) {
		free(allAttrs);
		delete[] data;
		return status;
	}
	RID rid;
	InsertFileScan ifs(&hf, status);
	if (status != OK) {
		free(allAttrs);
		delete[] data;
		return status;
	}
	status = ifs.insertRecord(rec, rid);
	//clean up
	free(allAttrs);
	delete[] data;
	if (status != OK)
		return status;

	
return OK;

}

